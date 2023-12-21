package bio.guoda.preston.server;

import bio.guoda.preston.RefNodeFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Map;

import static bio.guoda.preston.server.RedirectingServlet.ACTIVITY;
import static bio.guoda.preston.server.RedirectingServlet.CONTENT_ID;
import static org.hamcrest.MatcherAssert.assertThat;

public class RedirectingServletTest {


    @Test
    public void dealiasDOI() throws IOException, URISyntaxException {
        Map<String, String> contentId = RedirectingServlet.findMostRecentContentId(
                RefNodeFactory.toIRI("https://doi.org/10.15468/aomfnb"),
                RedirectingServlet.QUERY_TYPE_DOI, "https://lod.globalbioticinteractions.org/query"
        );
        assertThat(contentId.get(CONTENT_ID), Is.is("hash://sha256/1e2b7436fce1848f41698e5a9c193f311abaf0ee051bec1a2e48b5106d29524d"));
        assertThat(contentId.get(ACTIVITY), Is.is("urn:uuid:77f3faf7-acd2-4f14-9c0e-4e04ef5b63c7"));
    }

    @Test
    public void dealiasHash() throws IOException, URISyntaxException {
        Map<String, String> contentId = RedirectingServlet.findMostRecentContentId(
                RefNodeFactory.toIRI("hash://sha256/1e2b7436fce1848f41698e5a9c193f311abaf0ee051bec1a2e48b5106d29524d"),
                RedirectingServlet.QUERY_TYPE_HASH,
                "https://lod.globalbioticinteractions.org/query");
        assertThat(contentId.get(CONTENT_ID), Is.is("hash://sha256/1e2b7436fce1848f41698e5a9c193f311abaf0ee051bec1a2e48b5106d29524d"));

    }

    @Test
    public void dealiasUUID() throws IOException, URISyntaxException {
        Map<String, String> contentId = RedirectingServlet.findMostRecentContentId(
                RefNodeFactory.toIRI("urn:uuid:4fa7b334-ce0d-4e88-aaae-2e0c138d049e"),
                RedirectingServlet.QUERY_TYPE_UUID,
                "https://lod.globalbioticinteractions.org/query");
        assertThat(contentId.get(CONTENT_ID), Is.is("hash://sha256/1e2b7436fce1848f41698e5a9c193f311abaf0ee051bec1a2e48b5106d29524d"));

    }

    @Test
    public void dealiasURL() throws URISyntaxException, IOException {
        Map<String, String> contentId = RedirectingServlet.findMostRecentContentId(
                RefNodeFactory.toIRI("https://hosted-datasets.gbif.org/eBird/2022-eBird-dwca-1.0.zip"),
                RedirectingServlet.QUERY_TYPE_URL,
                "https://lod.globalbioticinteractions.org/query");
        assertThat(contentId.get(CONTENT_ID), Is.is("hash://sha256/1e2b7436fce1848f41698e5a9c193f311abaf0ee051bec1a2e48b5106d29524d"));
    }

    @Test
    public void parseResult() throws IOException {
        JsonNode response = new ObjectMapper().readTree(getClass().getResourceAsStream("url-response.json"));
        Map<String, String> actual = RedirectingServlet.extractProvenanceInfo(response);
        assertThat(actual.get(CONTENT_ID), Is.is("hash://sha256/1e2b7436fce1848f41698e5a9c193f311abaf0ee051bec1a2e48b5106d29524d"));
        assertThat(actual.get(RedirectingServlet.DOI), Is.is("https://doi.org/10.15468/aomfnb"));
        assertThat(actual.get(RedirectingServlet.UUID), Is.is("urn:uuid:4fa7b334-ce0d-4e88-aaae-2e0c138d049e"));
        assertThat(actual.get(RedirectingServlet.ARCHIVE_URL), Is.is("https://hosted-datasets.gbif.org/eBird/2022-eBird-dwca-1.0.zip"));
        assertThat(actual.get(RedirectingServlet.SEEN_AT), Is.is("2023-12-02T16:05:25.261Z"));
        assertThat(actual.get(RedirectingServlet.PROVENANCE_ID), Is.is("hash://sha256/5b7fa37bf8b64e7c935c4ff3389e36f8dd162f0705410dd719fd089e1ea253cd"));

    }

    @Test
    public void queryTypeDOI() {
        String s = RedirectingServlet.queryTypeForRequestedId("10.123/2345");
        assertThat(s, Is.is(RedirectingServlet.QUERY_TYPE_DOI));
    }

    @Test
    public void queryTypeUUID() {
        String s = RedirectingServlet.queryTypeForRequestedId("urn:uuid:4fa7b334-ce0d-4e88-aaae-2e0c138d049e");
        assertThat(s, Is.is(RedirectingServlet.QUERY_TYPE_UUID));
    }

    @Test
    public void queryTypeHash() {
        String s = RedirectingServlet.queryTypeForRequestedId("hash://sha256/1e2b7436fce1848f41698e5a9c193f311abaf0ee051bec1a2e48b5106d29524d");
        assertThat(s, Is.is(RedirectingServlet.QUERY_TYPE_HASH));
    }

    @Test
    public void queryTypeUrl() {
        String s = RedirectingServlet.queryTypeForRequestedId("https://example.org");
        assertThat(s, Is.is(RedirectingServlet.QUERY_TYPE_URL));
    }

}