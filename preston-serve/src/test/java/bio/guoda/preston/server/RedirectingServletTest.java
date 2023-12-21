package bio.guoda.preston.server;

import bio.guoda.preston.RefNodeFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;

import static org.hamcrest.MatcherAssert.assertThat;

public class RedirectingServletTest {


    @Test
    public void dealiasDOI() throws IOException, URISyntaxException {
        String contentId = RedirectingServlet.findMostRecentContentId(
                RefNodeFactory.toIRI("https://doi.org/10.15468/aomfnb"),
                "doi", "https://lod.globalbioticinteractions.org/query"
        );
        assertThat(contentId, Is.is("hash://sha256/1e2b7436fce1848f41698e5a9c193f311abaf0ee051bec1a2e48b5106d29524d"));
    }

    @Test
    public void dealiasHash() throws IOException, URISyntaxException {
        String contentId = RedirectingServlet.findMostRecentContentId(
                RefNodeFactory.toIRI("hash://sha256/1e2b7436fce1848f41698e5a9c193f311abaf0ee051bec1a2e48b5106d29524d"),
                "hash",
                "https://lod.globalbioticinteractions.org/query");
        assertThat(contentId, Is.is("hash://sha256/1e2b7436fce1848f41698e5a9c193f311abaf0ee051bec1a2e48b5106d29524d"));

    }

    @Test
    public void dealiasUUID() throws IOException, URISyntaxException {
        String contentId = RedirectingServlet.findMostRecentContentId(
                RefNodeFactory.toIRI("urn:uuid:4fa7b334-ce0d-4e88-aaae-2e0c138d049e"),
                "uuid",
                "https://lod.globalbioticinteractions.org/query");
        assertThat(contentId, Is.is("hash://sha256/1e2b7436fce1848f41698e5a9c193f311abaf0ee051bec1a2e48b5106d29524d"));

    }

    @Test
    public void dealiasURL() throws URISyntaxException, IOException {
        String contentId = RedirectingServlet.findMostRecentContentId(
                RefNodeFactory.toIRI("https://hosted-datasets.gbif.org/eBird/2022-eBird-dwca-1.0.zip"),
                "url",
                "https://lod.globalbioticinteractions.org/query");
        assertThat(contentId, Is.is("hash://sha256/1e2b7436fce1848f41698e5a9c193f311abaf0ee051bec1a2e48b5106d29524d"));
    }

    @Test
    public void parseResult() throws IOException {
        JsonNode response = new ObjectMapper().readTree(getClass().getResourceAsStream("url-response.json"));
        assertThat(RedirectingServlet.extractFirstContentId(response), Is.is("hash://sha256/1e2b7436fce1848f41698e5a9c193f311abaf0ee051bec1a2e48b5106d29524d"));

    }

    @Test
    public void queryTypeDOI() {
        String s = RedirectingServlet.queryTypeForRequestedId("10.123/2345");
        assertThat(s, Is.is("doi"));
    }

    @Test
    public void queryTypeUUID() {
        String s = RedirectingServlet.queryTypeForRequestedId("urn:uuid:4fa7b334-ce0d-4e88-aaae-2e0c138d049e");
        assertThat(s, Is.is("uuid"));
    }

    @Test
    public void queryTypeHash() {
        String s = RedirectingServlet.queryTypeForRequestedId("hash://sha256/1e2b7436fce1848f41698e5a9c193f311abaf0ee051bec1a2e48b5106d29524d");
        assertThat(s, Is.is("hash"));
    }

    @Test
    public void queryTypeUrl() {
        String s = RedirectingServlet.queryTypeForRequestedId("https://example.org");
        assertThat(s, Is.is("url"));
    }

}