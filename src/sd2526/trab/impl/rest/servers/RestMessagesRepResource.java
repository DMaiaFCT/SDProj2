package sd2526.trab.impl.rest.servers;

import java.util.List;
import java.util.logging.Logger;

import jakarta.ws.rs.ext.Provider;
import sd2526.trab.api.Message;
import sd2526.trab.api.java.Messages;
import sd2526.trab.api.rest.RestMessages;
import sd2526.trab.impl.api.java.AdminMessages;
import sd2526.trab.impl.api.rest.RestAdminMessages;
import sd2526.trab.impl.java.servers.JavaMessagesRep;

import jakarta.inject.Singleton;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;

@Provider
@Singleton
public class RestMessagesRepResource extends RestResource implements RestMessages, RestAdminMessages {

    private static final Logger Log = Logger.getLogger(RestMessagesRepResource.class.getName());

    @Context
    private HttpHeaders headers;

    private final Messages impl;

    public RestMessagesRepResource(JavaMessagesRep impl) {
        this.impl = impl;
    }

    @Override
    public String postMessage(String pwd, Message msg) {
        return super.resultOrThrow(impl.postMessage(pwd, msg));
    }

    @Override
    public Message getMessage(String name, String mid, String pwd) {
        return super.resultOrThrow(impl.getInboxMessage(name, mid, pwd));
    }

    @Override
    public List<String> getMessages(String name, String pwd, String query) {
        if (query != null && !query.isEmpty())
            return super.resultOrThrow(impl.searchInbox(name, pwd, query));
        else
            return super.resultOrThrow(impl.getAllInboxMessages(name, pwd));
    }

    @Override
    public void removeFromUserInbox(String name, String mid, String pwd) {
        super.resultOrThrow(impl.removeInboxMessage(name, mid, pwd));
    }

    @Override
    public void deleteMessage(String name, String mid, String pwd) {
        super.resultOrThrow(impl.deleteMessage(name, mid, pwd));
    }

    @Override
    public void remotePostMessage(Message m) {
        requireServerSecret();
        super.resultOrThrow(((AdminMessages) impl).remotePostMessage(m));
    }

    @Override
    public void remoteDeleteMessage(String mid) {
        requireServerSecret();
        super.resultOrThrow(((AdminMessages) impl).remoteDeleteMessage(mid));
    }

    @Override
    public void remoteDeleteUserInbox(String name) {
        requireServerSecret();
        super.resultOrThrow(((AdminMessages) impl).remoteDeleteUserInbox(name));
    }

    private void requireServerSecret() {
        String expected = System.getProperty("service.secret");
        String provided = headers.getHeaderString("X-Service-Secret");
        if (expected != null && !expected.equals(provided)) {
            Log.warning("Acesso não autorizado — secret inválido.");
            throw new WebApplicationException(Response.Status.FORBIDDEN);
        }
    }
}