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
	}

	public static void main(String[] args) throws UnknownHostException {
        String hostName = java.net.InetAddress.getLocalHost().getHostName();
        String resolvedKeyStore = "/home/sd/" + hostName + ".ks";

        System.setProperty("javax.net.ssl.keyStore", resolvedKeyStore);
        System.setProperty("javax.net.ssl.keyStorePassword", "password");
        System.setProperty("javax.net.ssl.trustStore", "/home/sd/truststore.ks");
        System.setProperty("javax.net.ssl.trustStorePassword", "changeit");

		new RestMessagesServer().start();
	}
}