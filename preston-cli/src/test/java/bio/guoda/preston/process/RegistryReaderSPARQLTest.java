package bio.guoda.preston.process;

import bio.guoda.preston.ResourcesHTTP;
import bio.guoda.preston.stream.ContentStreamUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpGet;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QueryParseException;
import org.apache.tika.io.IOUtils;
import org.hamcrest.core.Is;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static bio.guoda.preston.RefNodeFactory.toIRI;
import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

public class RegistryReaderSPARQLTest {

    @Ignore("order of authors changes so comparison over time is hard related to https://github.com/bio-guoda/preston/issues/137")
    @Test
    public void resultContainsExact() throws URISyntaxException, IOException {
        String result = assertResultContains(getQuery());

        // the order of the results in not consistent
        // related to
        assertThat(StringUtils.trim(result),
                Is.is(IOUtils.toString(getClass().getResourceAsStream("author.sparql.result.json"), StandardCharsets.UTF_8.name())));
    }

    @Test
    public void resultContainsValid() throws URISyntaxException, IOException {
        assertResultContains(getQueryWithPrefixes());
    }

    @Test
    public void detectValidSPARQL() throws IOException {
        String queryWithPrefixes = getQueryWithPrefixes();
        assertTrue(isSPARQLQuery(queryWithPrefixes));
    }

    private boolean isSPARQLQuery(String queryWithPrefixes) {
        Query query = QueryFactory.create(queryWithPrefixes);
        return query != null;
    }

    @Ignore("Scholia queries in https://github.com/bio-guoda/preston/issues/137 do not include prefixes")
    @Test
    public void detectActualSPARQL() throws IOException {
        assertTrue(isSPARQLQuery(getQuery()));
    }

    @Test(expected = QueryParseException.class)
    public void invalidQuery() throws IOException {
        isSPARQLQuery("mickey mouse");
    }

    @Test
    public void resultContains() throws URISyntaxException, IOException {
        assertResultContains(getQuery());
    }

    private String assertResultContains(String query) throws IOException, URISyntaxException {

        URI url = new URI(
                "https",
                "query.wikidata.org",
                "/sparql",
                "query=" + query, null);

        HttpGet get = new HttpGet(url);
        get.setHeader(HttpHeaders.ACCEPT, "application/sparql-results+json");
        get.setHeader(HttpHeaders.ACCEPT_ENCODING, "gzip");

        InputStream inputStream = ResourcesHTTP.asInputStream(
                toIRI(url),
                get,
                Collections.emptyList(),
                ContentStreamUtil.getNOOPDerefProgressListener());

        String result = IOUtils.toString(inputStream, StandardCharsets.UTF_8.name());

        assertThat(result, containsString("scholarly article"));
        return result;
    }

    private String getQuery() {
        try {
            return IOUtils.toString(
                        getClass().getResourceAsStream("/bio/guoda/preston/process/author.sparql"),
                        StandardCharsets.UTF_8.name());
        } catch (IOException e) {
            fail("failed to load test fixture");
            throw new IllegalStateException(e);
        }
    }

    private String getQueryWithPrefixes() {
        try {
            return IOUtils.toString(
                        getClass().getResourceAsStream("/bio/guoda/preston/process/author-with-prefixes.sparql"),
                        StandardCharsets.UTF_8.name());
        } catch (IOException e) {
            fail("failed to load test fixture");
            throw new IllegalStateException(e);
        }
    }


}