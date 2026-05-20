package sd2526.trab.impl.rest.servers;

import java.net.URI;
import java.net.UnknownHostException;
import java.util.logging.Logger;

import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import sd2526.trab.api.java.Messages;
import sd2526.trab.impl.discovery.Discovery;

public class RestZohoServer extends AbstractRestServer {

    public static final int PORT = 4569;

    private static final Logger Log = Logger.getLogger(RestZohoServer.class.getName());

    RestZohoServer() throws UnknownHostException {
        super(Log, Messages.SERVICE_NAME, PORT);
    }

    @Override
    void registerResources(ResourceConfig config) {
        config.register(RestZohoResource.class);
    }

    @Override
    protected void start() {
        try {
            ResourceConfig config = new ResourceConfig();
            registerResources(config);

            var uri = URI.create("https://0.0.0.0:%s/rest".formatted(port));
            JdkHttpServerFactory.createHttpServer(uri, config, javax.net.ssl.SSLContext.getDefault());

            if (service != null)
                Discovery.getInstance().announce(serviceName(), super.serverURI);

            Log.info(String.format("%s ZohoServer ready @ %s\n", service, serverURI));
        } catch (Exception e) {
            Log.severe("Failed to start Zoho REST server: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws UnknownHostException {
        String hostName = java.net.InetAddress.getLocalHost().getHostName();
        String resolvedKeyStore = "/home/sd/" + hostName + ".ks";

        System.setProperty("javax.net.ssl.keyStore", resolvedKeyStore);
        System.setProperty("javax.net.ssl.keyStorePassword", "password");
        System.setProperty("javax.net.ssl.trustStore", "/home/sd/truststore.ks");
        System.setProperty("javax.net.ssl.trustStorePassword", "changeit");
        new RestZohoServer().start();
    }
}