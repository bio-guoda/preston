package bio.guoda.preston.process;

import bio.guoda.preston.ResourcesHTTP;
import bio.guoda.preston.stream.ContentStreamUtil;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpGet;
import org.hamcrest.core.Is;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static bio.guoda.preston.RefNodeFactory.toIRI;
import static junit.framework.TestCase.fail;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

public class RegistryReaderSPARQLIT {

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
    public void resultContains() throws URISyntaxException, IOException {
        assertResultContains(getQuery());
    }

    private String assertResultContains(String query) throws IOException, URISyntaxException {

        String result = execute(query);

        assertThat(result, containsString("scholarly article"));
        return result;
    }

    private String execute(String query) throws URISyntaxException, IOException {
        URI url = new URI(
                "https",
                "query.wikidata.org",
                "/sparql",
                "query=" + query, null);

        HttpGet get = new HttpGet(url);
        get.setHeader(HttpHeaders.ACCEPT, "application/sparql-results+json");

        InputStream inputStream = ResourcesHTTP.asInputStream(
                toIRI(url),
                get,
                ContentStreamUtil.getNOOPDerefProgressListener(),
                ResourcesHTTP.NEVER_IGNORE);

        return IOUtils.toString(inputStream, StandardCharsets.UTF_8.name());
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


    @Test
    public void extractSPARQLQueriesFromScholiaPages() throws IOException {
        InputStream scholiaHtml = getClass().getResourceAsStream("scholia-ted-nelson-Q62852.html");
        String s = IOUtils.toString(scholiaHtml, StandardCharsets.UTF_8.name());

        List<String> results = new ArrayList<>();

        String[] chunks = StringUtils.splitByWholeSeparatorPreserveAllTokens(s, "`");
        for (String chunk : chunks) {
            if (StringUtils.contains(chunk, "SELECT")) {
                String prefix = "PREFIX wd: <http://www.wikidata.org/entity/>\n" +
                        "PREFIX wdt: <http://www.wikidata.org/prop/direct/>\n" +
                        "PREFIX wikibase: <http://wikiba.se/ontology#>\n" +
                        "PREFIX p: <http://www.wikidata.org/prop/>\n" +
                        "PREFIX ps: <http://www.wikidata.org/prop/statement/>\n" +
                        "PREFIX pq: <http://www.wikidata.org/prop/qualifier/>\n" +
                        "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                        "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                        "PREFIX bd: <http://www.bigdata.com/rdf#>\n";
                String queryWithPrefixes = prefix + chunk;
                try {
                    String execute = execute(queryWithPrefixes);
                    results.add(execute);
                } catch (URISyntaxException ex) {
                    ex.printStackTrace();
                    fail("failed to parseQuads: " + queryWithPrefixes);

                }
            }
        }
        // nineteen sparql queries expected in the page
        assertThat(results.size(), Is.is(19));
    }


}