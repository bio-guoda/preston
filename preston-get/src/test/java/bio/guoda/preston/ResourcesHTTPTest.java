package bio.guoda.preston;

import org.apache.http.client.methods.HttpGet;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.MatcherAssert.assertThat;

public class ResourcesHTTPTest {

    @Test
    public void getLocalFile() throws URISyntaxException, IOException {
        URL resource = getClass().getResource("/bio/guoda/preston/plazidwca.zip");
        try (InputStream inputStream = ResourcesHTTP.asInputStreamOfflineOnly(RefNodeFactory.toIRI(resource.toURI()))) {
            assertThat(inputStream, is(not(nullValue())));
        }
    }

    @Test
    public void getNonExistingLocalFile() throws IOException {
        URL resource = getClass().getResource("/bio/guoda/preston/plazidwca.zip");
        URI nonExistingResource = URI.create(resource.toExternalForm() + "does-not-exist");
        assertThat(new File(nonExistingResource).exists(), is(false));
        try (InputStream inputStream = ResourcesHTTP.asInputStreamOfflineOnly(RefNodeFactory.toIRI(nonExistingResource))) {
            assertThat(inputStream, is(nullValue()));
        }
    }

    @Test
    public void setAuthorizationNotAdd() {
        HttpGet httpGet = new HttpGet("https://example.org");
        assertThat(httpGet.getAllHeaders().length, is(0));
        ResourcesHTTP.setAuthBearerIfAvailable(httpGet, "1234");
        assertThat(httpGet.getAllHeaders().length, is(1));
        assertThat(httpGet.getFirstHeader("Authorization").toString(), is("Authorization: Bearer 1234"));
        ResourcesHTTP.setAuthBearerIfAvailable(httpGet, "4567");
        assertThat(httpGet.getAllHeaders().length, is(1));
        assertThat(httpGet.getFirstHeader("Authorization").toString(), is("Authorization: Bearer 4567"));
    }

    @Test
    public void userAgent() {
        String userAgent = ResourcesHTTP.getUserAgent("1.1.1");
        assertThat(userAgent, is("globalbioticinteractions/1.1.1 (https://globalbioticinteractions.org; mailto:info@globalbioticinteractions.org)"));
    }

}