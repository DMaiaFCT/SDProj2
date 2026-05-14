package sd2526.trab.impl.grpc.servers;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.logging.Logger;

import io.grpc.Server;
import sd2526.trab.impl.discovery.Discovery;
import sd2526.trab.impl.java.servers.AbstractServer;

import java.io.FileInputStream;
import java.security.KeyStore;
import javax.net.ssl.KeyManagerFactory;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

public abstract class AbstractGrpcServer extends AbstractServer {

    public static final Context.Key<String> SECRET_CONTEXT_KEY = Context.key("X-Service-Secret");
    private static final Metadata.Key<String> SECRET_META_KEY = Metadata.Key.of("X-Service-Secret", Metadata.ASCII_STRING_MARSHALLER);

    private static final String SERVER_BASE_URI = "grpc://%s:%s%s";
    private static final String GRPC_CTX = "/grpc";

    protected final Server server;

    protected AbstractGrpcServer(Logger log, String service, int port) throws UnknownHostException {
        super(log, service, String.format(SERVER_BASE_URI,
                java.net.InetAddress.getLocalHost().getHostName(), port, GRPC_CTX), port);

        try {
            String ksFile = System.getProperty("javax.net.ssl.keyStore");
            String ksPass = System.getProperty("javax.net.ssl.keyStorePassword");

            if (ksFile == null || ksPass == null) {
                log.severe("CRITICAL: Keystore properties are missing. Server will likely fail to start securely.");
            }

            KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
            try (var is = new FileInputStream(ksFile)) {
                ks.load(is, ksPass.toCharArray());
            }

            var kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(ks, ksPass.toCharArray());
            SslContext sslContext = GrpcSslContexts.configure(
                    SslContextBuilder.forServer(kmf)
            ).build();

            var builder = NettyServerBuilder.forPort(port).sslContext(sslContext);

            builder.intercept(new ServerInterceptor() {
                @Override
                public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                        ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

                    String secret = headers.get(SECRET_META_KEY);

                    Context ctx = Context.current().withValue(SECRET_CONTEXT_KEY, secret);
                    return Contexts.interceptCall(ctx, call, headers, next);
                }
            });

            for( var s : controllers( super.serverURI ) ) {
                builder.addService( s );
            }

            this.server = builder.build();
            sd2526.trab.impl.db.Hibernate.getInstance();

        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize gRPC TLS: " + e.getMessage(), e);
        }
    }

    protected abstract List<GrpcController> controllers( String uri );

    protected void start() throws IOException {
        Discovery.getInstance().announce(serviceName(), super.serverURI);
        Log.info(String.format("%s gRPC Server ready @ %s\n", service, serverURI));

        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            System.err.println("*** shutting down gRPC server since JVM is shutting down");
            server.shutdownNow();
            System.err.println("*** server shut down");
        }));
    }
}