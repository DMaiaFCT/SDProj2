package sd2526.trab.impl.rest.servers;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.ext.Provider;
import java.io.IOException;

@Provider
public class VersionHeaderHandler implements ContainerResponseFilter, ContainerRequestFilter {

    public static final String HEADER_VERSION = "X-MESSAGES-version";
    public static final ThreadLocal<Long> version = new ThreadLocal<>();

    @Override
    public void filter(ContainerRequestContext reqCtx) throws IOException {
        String value = reqCtx.getHeaderString(HEADER_VERSION);
        if (value != null && !value.isEmpty()) {
            version.set(Long.valueOf(value));
        } else {
            version.set(0L);
        }
    }

    @Override
    public void filter(ContainerRequestContext reqCtx, ContainerResponseContext resCtx) throws IOException {
        Long value = version.get();
        if (value != null) {
            resCtx.getHeaders().add(HEADER_VERSION, Long.toString(value));
        }
        version.remove();
    }
}