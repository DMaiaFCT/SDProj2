package sd2526.trab.impl.rest.servers;

import java.net.UnknownHostException;
import java.util.logging.Logger;

import org.glassfish.jersey.server.ResourceConfig;

import sd2526.trab.api.java.Messages;

public class RestMessagesServer extends AbstractRestServer {
    public static final int PORT = 4567;

    private static Logger Log = Logger.getLogger(RestMessagesServer.class.getName());

    RestMessagesServer() throws UnknownHostException {
        super(Log, Messages.SERVICE_NAME, PORT);
    }

    @Override
    void registerResources(ResourceConfig config) {
        config.register(RestMessagesResource.class);
        config.register(VersionHeaderHandler.class);

    }

    public static void main(String[] args) throws UnknownHostException {
        new RestMessagesServer().start();
    }
}