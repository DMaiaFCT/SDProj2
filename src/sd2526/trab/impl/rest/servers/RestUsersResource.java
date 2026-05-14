package sd2526.trab.impl.rest.servers;

import java.util.List;
import java.util.Set;

import jakarta.inject.Singleton;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import java.util.logging.Logger;
import sd2526.trab.api.User;
import sd2526.trab.api.java.Users;
import sd2526.trab.api.rest.RestUsers;
import sd2526.trab.impl.api.java.AdminUsers;
import sd2526.trab.impl.api.rest.RestAdminUsers;
import sd2526.trab.impl.java.clients.Clients;
import sd2526.trab.impl.java.servers.JavaUsers;

@Singleton
public class RestUsersResource extends RestResource implements RestUsers, RestAdminUsers {

    private static final Logger Log = Logger.getLogger(RestUsersResource.class.getName());

    @Context
    private HttpHeaders headers;

	static boolean isGateway = false;
	
	Users impl;	

	synchronized Users impl() {
		if( impl == null )
			impl = isGateway ? Clients.UsersClient.get() : JavaUsers.getInstance();	
		return impl;
	}
		
	public RestUsersResource() {}
	
	RestUsersResource(boolean gw) {	
		isGateway = gw;
	}
	
	@Override
	public String postUser(User user) {
		return super.resultOrThrow( impl().postUser(user));
	}

	@Override
	public User getUser(String name, String pwd) {
		return super.resultOrThrow( impl().getUser(name, pwd));
	}

	@Override
	public User updateUser(String name, String pwd, User info) {
		return super.resultOrThrow( impl().updateUser(name, pwd, info));
	}

	@Override
	public User deleteUser(String name, String pwd) {
		return super.resultOrThrow( impl().deleteUser(name, pwd));
	}

	@Override
	public List<User> searchUsers(String name, String pwd, String pattern) {
		return super.resultOrThrow( impl().searchUsers(name, pwd, pattern));
	}

	@Override
	public Set<String> checkUsers(Set<String> names) {
        requireServerSecret();
		return super.resultOrThrow(((AdminUsers)impl).checkUsers(names));
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
