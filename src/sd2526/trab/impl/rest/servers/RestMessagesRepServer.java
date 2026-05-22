package sd2526.trab.impl.rest.servers;

import java.net.UnknownHostException;
import java.util.logging.Logger;

import org.glassfish.jersey.server.ResourceConfig;

import sd2526.trab.api.java.Messages;
import sd2526.trab.impl.java.servers.JavaMessagesRep;

public class RestMessagesRepServer extends AbstractRestServer {

    public static final int PORT = 4568;

    private static final Logger Log = Logger.getLogger(RestMessagesRepServer.class.getName());

    // A instância da resource é criada aqui — não lazy — para que o
    // JavaMessagesRep (e o Kafka subscriber) arranque ao iniciar o servidor
    private final RestMessagesRepResource resource;

    RestMessagesRepServer() throws UnknownHostException {
        super(Log, Messages.SERVICE_NAME, PORT);
        // Força a criação do JavaMessagesRep imediatamente (e do Kafka subscriber)
        this.resource = new RestMessagesRepResource(JavaMessagesRep.getInstance());
    }

    @Override
    void registerResources(ResourceConfig config) {
        // registerInstances em vez de register — garante que usamos a instância
        // criada acima, com o Kafka já inicializado
        config.registerInstances(resource);
        config.register(VersionHeaderHandler.class);
    }

    public static void main(String[] args) throws UnknownHostException {
        String hostName = java.net.InetAddress.getLocalHost().getHostName();
        String resolvedKeyStore = "/home/sd/" + hostName + ".ks";

        System.setProperty("javax.net.ssl.keyStore", resolvedKeyStore);
        System.setProperty("javax.net.ssl.keyStorePassword", "password");
        System.setProperty("javax.net.ssl.trustStore", "/home/sd/truststore.ks");
        System.setProperty("javax.net.ssl.trustStorePassword", "changeit");

        new RestMessagesRepServer().start();
    }
}