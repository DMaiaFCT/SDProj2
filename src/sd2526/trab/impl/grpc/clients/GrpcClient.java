package sd2526.trab.impl.grpc.clients;

import static sd2526.trab.api.java.Result.error;
import static sd2526.trab.api.java.Result.ok;
import static sd2526.trab.api.java.Result.ErrorCode.INTERNAL_ERROR;



import java.net.URI;
import java.util.function.Supplier;
import java.io.FileInputStream;
import java.security.KeyStore;
import javax.net.ssl.TrustManagerFactory;

import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

import sd2526.trab.api.java.Result;
import sd2526.trab.api.java.Result.ErrorCode;

public class GrpcClient {

    final protected URI serverURI;
    final protected Channel channel;

    protected GrpcClient(String serverUrl) {
        this.serverURI = URI.create(serverUrl);

        try {
            String trustStoreFilename = System.getProperty("javax.net.ssl.trustStore");
            String trustStorePassword = System.getProperty("javax.net.ssl.trustStorePassword");

            if (trustStoreFilename == null || trustStorePassword == null) {
                System.err.println("WARNING: Truststore properties missing. Client may fail to connect.");
            }

            KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
            try (FileInputStream input = new FileInputStream(trustStoreFilename)) {
                trustStore.load(input, trustStorePassword.toCharArray());
            }

            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(trustStore);

            SslContext context = GrpcSslContexts.configure(
                    SslContextBuilder.forClient().trustManager(trustManagerFactory)
            ).build();

            Metadata headers = new Metadata();
            String secret = System.getProperty("service.secret");
            if (secret != null) {
                headers.put(Metadata.Key.of("X-Service-Secret", Metadata.ASCII_STRING_MARSHALLER), secret);
            }

            this.channel = NettyChannelBuilder.forAddress(serverURI.getHost(), serverURI.getPort())
                    .sslContext(context)
                    .intercept(MetadataUtils.newAttachHeadersInterceptor(headers))
                    .enableRetry()
                    .build();

        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize gRPC client SSL context: " + e.getMessage(), e);
        }
    }

    protected <T> Result<T> toJavaResult(Supplier<T> func) {
        try {
            return ok(func.get());
        } catch (StatusRuntimeException sre) {
            return error(statusToErrorCode(sre.getStatus()));
        } catch (Exception x) {
            x.printStackTrace();
            return Result.error(INTERNAL_ERROR);
        }
    }

    protected Result<Void> toJavaResult(Runnable proc) {
        return toJavaResult( () -> {
            proc.run();
            return null;
        } );
    }

    protected static ErrorCode statusToErrorCode(Status status) {
        return switch (status.getCode()) {
            case OK -> ErrorCode.OK;
            case NOT_FOUND -> ErrorCode.NOT_FOUND;
            case ALREADY_EXISTS -> ErrorCode.CONFLICT;
            case PERMISSION_DENIED -> ErrorCode.FORBIDDEN;
            case INVALID_ARGUMENT -> ErrorCode.BAD_REQUEST;
            case UNIMPLEMENTED -> ErrorCode.NOT_IMPLEMENTED;
            default -> ErrorCode.INTERNAL_ERROR;
        };
    }

    @Override
    public String toString() {
        return serverURI.toString();
    }
}