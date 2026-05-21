package sd2526.trab.impl.rest.servers;

import java.net.UnknownHostException;
import java.util.logging.Logger;

import org.glassfish.jersey.server.ResourceConfig;

import sd2526.trab.api.java.Messages;
import sd2526.trab.impl.java.servers.JavaZohoMessages;

public class RestZohoServer extends AbstractRestServer {

    public static final int PORT = 4569;

    private static final Logger Log = Logger.getLogger(RestZohoServer.class.getName());

    RestZohoServer() throws UnknownHostException {
        super(Log, Messages.SERVICE_NAME, PORT);
    }

    @Override
    void registerResources(ResourceConfig config) {
        config.registerInstances(RestMessagesResource.class);
        config.register(VersionHeaderHandler.class);
    }

    public static void main(String[] args) throws UnknownHostException {
        String hostName = java.net.InetAddress.getLocalHost().getHostName();
        String resolvedKeyStore = "/home/sd/" + hostName + ".ks";

        System.setProperty("javax.net.ssl.keyStore", resolvedKeyStore);
        System.setProperty("javax.net.ssl.keyStorePassword", "password");
        System.setProperty("javax.net.ssl.trustStore", "/home/sd/truststore.ks");
        System.setProperty("javax.net.ssl.trustStorePassword", "changeit");

        // O tester passa "true" para arranque limpo, "false" para restart com estado
        boolean cleanState = args.length == 0 || Boolean.parseBoolean(args[0]);
        JavaZohoMessages.init(cleanState);

        new RestZohoServer().start();
    }
}