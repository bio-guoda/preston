package bio.guoda.preston.server;

import bio.guoda.preston.RefNodeFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static bio.guoda.preston.server.RedirectingServlet.CONTENT_ID;
import static bio.guoda.preston.server.RedirectingServlet.CONTENT_TYPE;
import static bio.guoda.preston.server.RedirectingServlet.getContentType;
import static org.hamcrest.MatcherAssert.assertThat;

public class ProvUtilTest {

    @Test
    public void parseResult() throws IOException {
        JsonNode response = new ObjectMapper().readTree(getClass().getResourceAsStream("url-response.json"));
        Map<String, String> actual = ProvUtil.extractProvenanceInfo(response);
        assertThat(actual.get(CONTENT_ID), Is.is("hash://sha256/1e2b7436fce1848f41698e5a9c193f311abaf0ee051bec1a2e48b5106d29524d"));
        assertThat(actual.get(CONTENT_TYPE), Is.is("application/dwca"));
        assertThat(actual.get(RedirectingServlet.DOI), Is.is("https://doi.org/10.15468/aomfnb"));
        assertThat(actual.get(RedirectingServlet.UUID), Is.is("urn:uuid:4fa7b334-ce0d-4e88-aaae-2e0c138d049e"));
        assertThat(actual.get(RedirectingServlet.ARCHIVE_URL), Is.is("https://hosted-datasets.gbif.org/eBird/2022-eBird-dwca-1.0.zip"));
        assertThat(actual.get(RedirectingServlet.SEEN_AT), Is.is("2023-12-02T16:05:25.261Z"));
        assertThat(actual.get(RedirectingServlet.PROVENANCE_ID), Is.is("hash://sha256/5b7fa37bf8b64e7c935c4ff3389e36f8dd162f0705410dd719fd089e1ea253cd"));
    }

    @Test
    public void generateQueryDOI() throws IOException {
        String queryForDOI = ProvUtil.generateQuery(
                RefNodeFactory.toIRI("https://doi.org/10.123/345"),
                "doi",
                "application/dwca",
                RefNodeFactory.toIRI("hash://sha256/5b7fa37bf8b64e7c935c4ff3389e36f8dd162f0705410dd719fd089e1ea253cd"),
                false);

        assertThat(queryForDOI,
                Is.is(IOUtils.toString(getClass().getResourceAsStream("doi-example.sparql"), StandardCharsets.UTF_8))
        );

    }

    @Test
    public void generateQueryUUID() throws IOException {
        String queryForDOI = ProvUtil.generateQuery(
                RefNodeFactory.toIRI("urn:uuid:a44859c6-af4f-4a2a-a184-1b2d68c82099"),
                "uuid",
                "application/dwca",
                RefNodeFactory.toIRI("hash://sha256/5b7fa37bf8b64e7c935c4ff3389e36f8dd162f0705410dd719fd089e1ea253cd"),
                false);

        assertThat(queryForDOI,
                Is.is(IOUtils.toString(getClass().getResourceAsStream("uuid-example.sparql"), StandardCharsets.UTF_8))
        );

    }

    @Test
    public void generateQueryURL() throws IOException {
        String queryForDOI = ProvUtil.generateQuery(
                RefNodeFactory.toIRI("https://example.org"),
                "url",
                "application/dwca",
                RefNodeFactory.toIRI("hash://sha256/5b7fa37bf8b64e7c935c4ff3389e36f8dd162f0705410dd719fd089e1ea253cd"),
                false);

        assertThat(queryForDOI,
                Is.is(IOUtils.toString(getClass().getResourceAsStream("url-example.sparql"), StandardCharsets.UTF_8))
        );

    }

    @Test
    public void queryTypeDOI() {
        String s = ProvUtil.queryTypeForRequestedId("10.123/2345");
        assertThat(s, Is.is(ProvUtil.QUERY_TYPE_DOI));
    }

    @Test
    public void queryTypeUUID() {
        String s = ProvUtil.queryTypeForRequestedId("urn:uuid:4fa7b334-ce0d-4e88-aaae-2e0c138d049e");
        assertThat(s, Is.is(ProvUtil.QUERY_TYPE_UUID));
    }

    @Test
    public void queryTypeHash() {
        String s = ProvUtil.queryTypeForRequestedId("hash://sha256/1e2b7436fce1848f41698e5a9c193f311abaf0ee051bec1a2e48b5106d29524d");
        assertThat(s, Is.is(ProvUtil.QUERY_TYPE_CONTENT_ID));
    }

    @Test
    public void queryTypeUrl() {
        String s = ProvUtil.queryTypeForRequestedId("https://example.org");
        assertThat(s, Is.is(ProvUtil.QUERY_TYPE_URL));
    }

}