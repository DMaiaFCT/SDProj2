package sd2526.trab.impl.rest.servers;

import java.util.List;
import java.util.logging.Logger;

import jakarta.inject.Singleton;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;

import sd2526.trab.api.Message;
import sd2526.trab.api.rest.RestMessages;
import sd2526.trab.impl.api.rest.RestAdminMessages;
import sd2526.trab.impl.java.servers.JavaZohoMessages;

@Singleton
public class RestZohoResource extends RestResource implements RestMessages, RestAdminMessages {

    private static final Logger Log = Logger.getLogger(RestZohoResource.class.getName());

    @Context
    private HttpHeaders headers;

    public RestZohoResource() {
    }

    private JavaZohoMessages impl() {
        return JavaZohoMessages.getInstance();
    }


    @Override
    public String postMessage(String pwd, Message msg) {
        Log.info(() -> "postMessage : pwd = %s, msg = %s\n".formatted(pwd, msg));
        return super.resultOrThrow(impl().postMessage(pwd, msg));
    }

    @Override
    public Message getMessage(String name, String mid, String pwd) {
        Log.info(() -> "getMessage : name = %s, mid = %s\n".formatted(name, mid));
        return super.resultOrThrow(impl().getInboxMessage(name, mid, pwd));
    }

    @Override
    public List<String> getMessages(String name, String pwd, String query) {
        Log.info(() -> "getMessages : name = %s, query = %s\n".formatted(name, query));
        if (query != null && !query.isEmpty())
            return super.resultOrThrow(impl().searchInbox(name, pwd, query));
        else
            return super.resultOrThrow(impl().getAllInboxMessages(name, pwd));
    }

    @Override
    public void removeFromUserInbox(String name, String mid, String pwd) {
        Log.info(() -> "removeFromUserInbox : name = %s, mid = %s\n".formatted(name, mid));
        super.resultOrThrow(impl().removeInboxMessage(name, mid, pwd));
    }

    @Override
    public void deleteMessage(String name, String mid, String pwd) {
        Log.info(() -> "deleteMessage : name = %s, mid = %s\n".formatted(name, mid));
        super.resultOrThrow(impl().deleteMessage(name, mid, pwd));
    }

    @Override
    public void remotePostMessage(Message m) {
        requireServerSecret();
        super.resultOrThrow(impl().remotePostMessage(m));
    }

    @Override
    public void remoteDeleteMessage(String mid) {
        requireServerSecret();
        super.resultOrThrow(impl().remoteDeleteMessage(mid));
    }

    @Override
    public void remoteDeleteUserInbox(String name) {
        requireServerSecret();
        super.resultOrThrow(impl().remoteDeleteUserInbox(name));
    }


    private void requireServerSecret() {
        String expectedSecret = System.getProperty("service.secret");
        String providedSecret = headers.getHeaderString("X-Service-Secret");

        if (expectedSecret != null && !expectedSecret.equals(providedSecret)) {
            Log.warning("Blocked unauthorized access attempt. Invalid or missing secret.");
            throw new WebApplicationException(Response.Status.FORBIDDEN);
        }
    }
}