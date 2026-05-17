package sd2526.trab.impl.rest.servers;

import java.net.UnknownHostException;
import java.util.logging.Logger;

import org.glassfish.jersey.server.ResourceConfig;

public class RestGatewayServer extends AbstractRestServer {

	public static final int PORT = 6666;

	private static Logger Log = Logger.getLogger(RestGatewayServer.class.getName());

	RestGatewayServer() throws UnknownHostException {
		super(Log, null, PORT);
	}

	@Override
	void registerResources(ResourceConfig config) {
		config.registerInstances(new RestUsersResource(true), new RestMessagesResource(true));
//		config.register(.getClass());
//		config.register(.getClass());
	}

	public static void main(String[] args) throws UnknownHostException {
        System.setProperty("javax.net.ssl.keyStore", "/home/sd/users-domain-server.ks");
        System.setProperty("javax.net.ssl.keyStorePassword", "password");
        System.setProperty("javax.net.ssl.trustStore", "/home/sd/truststore.ks");
        System.setProperty("javax.net.ssl.trustStorePassword", "changeit");

		new RestGatewayServer().start();
	}
}