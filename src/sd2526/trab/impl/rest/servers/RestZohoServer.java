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
        config.registerInstances(new RestZohoResource());
        config.register(VersionHeaderHandler.class);
    }

    public static void main(String[] args) throws UnknownHostException {
        boolean cleanState = args.length == 0 || Boolean.parseBoolean(args[0]);
        JavaZohoMessages.init(cleanState);
        new RestZohoServer().start();
    }
}