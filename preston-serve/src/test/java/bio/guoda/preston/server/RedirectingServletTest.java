package bio.guoda.preston.server;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class RedirectingServletTest {

    @Test
    public void parseQuery() {
        String contentType = RedirectingServlet.getContentType("type=application/bla");
        assertThat(contentType, is("application/bla"));
    }

    @Test
    public void parseQueryEML() {
        String contentType = RedirectingServlet.getContentType("type=application/eml");
        assertThat(contentType, is("application/eml"));
    }

    @Test
    public void parseQueryDWCA() {
        String contentType = RedirectingServlet.getContentType("type=application/dwca");
        assertThat(contentType, is("application/dwca"));
    }

}