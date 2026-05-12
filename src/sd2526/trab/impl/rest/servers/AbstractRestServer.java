package sd2526.trab.impl.rest.servers;

import java.net.URI;
import java.util.logging.Logger;

import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import sd2526.trab.impl.discovery.Discovery;
import sd2526.trab.impl.java.servers.AbstractServer;
import sd2526.trab.impl.utils.IP;



public abstract class AbstractRestServer extends AbstractServer {
	private static final String SERVER_BASE_URI = "http://%s:%s%s";
	private static final String REST_CTX = "/rest";

	protected AbstractRestServer(Logger log, String service, int port) {
		super(log, service, String.format(SERVER_BASE_URI, IP.hostAddress(), port, REST_CTX));
	}

	protected void start() {
		sd2526.trab.impl.db.Hibernate.getInstance();
		
		ResourceConfig config = new ResourceConfig();
		registerResources( config );
		
		JdkHttpServerFactory.createHttpServer( URI.create(serverURI.replace(IP.hostAddress(), INETADDR_ANY)), config);
		
		if( service != null )
			Discovery.getInstance().announce(serviceName(), super.serverURI);
		
		Log.info(String.format("%s Server ready @ %s\n",  service, serverURI));
	}
	
	abstract void registerResources( ResourceConfig config );
}
