package bio.guoda.preston.server;

import org.junit.Test;

import java.net.URI;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class RedirectingServletTest {

    @Test
    public void parseQuery() {
        URI requestURI = URI.create("http://localhost:8080/badge/10.15468/w6hvhv?type=application/bla");
        String contentType = RedirectingServlet.getContentType(requestURI);
        assertThat(contentType, is("application/bla"));
    }

    @Test
    public void parseQueryEML() {
        URI requestURI = URI.create("http://localhost:8080/badge/10.15468/w6hvhv?type=application/eml");
        String contentType = RedirectingServlet.getContentType(requestURI);
        assertThat(contentType, is("application/eml"));
    }

    @Test
    public void parseQueryDWCA() {
        URI requestURI = URI.create("http://localhost:8080/badge/10.15468/w6hvhv?type=application/dwca");
        String contentType = RedirectingServlet.getContentType(requestURI);
        assertThat(contentType, is("application/dwca"));
    }

}