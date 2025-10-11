package bio.guoda.preston.server;

import bio.guoda.preston.RefNodeFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static bio.guoda.preston.server.RedirectingServlet.CONTENT_ID;
import static bio.guoda.preston.server.RedirectingServlet.CONTENT_TYPE;
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
    public void parseHistoryResult() throws IOException {
        JsonNode response = new ObjectMapper().readTree(getClass().getResourceAsStream("doi-history-response.json"));
        List<JsonNode> entries = new ArrayList<>();
        ProvUtil.extractProvenanceInfo(response, new Consumer<JsonNode>() {

            @Override
            public void accept(JsonNode jsonNode) {
                entries.add(jsonNode);
            }
        }, 1024);

        assertThat(entries.size(), Is.is(166));
        JsonNode actual = entries.get(0);
        assertThat(actual.get(CONTENT_ID).asText(), Is.is("hash://sha256/3e776052700d0eebeee73cc9b54fe6defcc857b14b7d4385cab397886c2d2b7e"));
        assertThat(actual.get(CONTENT_TYPE).asText(), Is.is("application/dwca"));
        assertThat(actual.get(RedirectingServlet.DOI).asText(), Is.is("https://doi.org/10.15468/oirgxw"));
        assertThat(actual.get(RedirectingServlet.UUID).asText(), Is.is("urn:uuid:b15d4952-7d20-46f1-8a3e-556a512b04c5"));
        assertThat(actual.get(RedirectingServlet.ARCHIVE_URL).asText(), Is.is("https://ipt.vertnet.org/archive.do?r=msb_mamm"));
        assertThat(actual.get(RedirectingServlet.SEEN_AT).asText(), Is.is("2025-06-01T22:48:51.161Z"));
        assertThat(actual.get(RedirectingServlet.PROVENANCE_ID).asText(), Is.is("hash://sha256/5b7fa37bf8b64e7c935c4ff3389e36f8dd162f0705410dd719fd089e1ea253cd"));
    }


    @Test
    public void generateQueryDOI() throws IOException {
        String queryForDOI = ProvUtil.generateQuery(
                RefNodeFactory.toIRI("https://doi.org/10.15468/oirgxw"),
                "doi",
                "application/dwca",
                RefNodeFactory.toIRI("hash://sha256/5b7fa37bf8b64e7c935c4ff3389e36f8dd162f0705410dd719fd089e1ea253cd"),
                false, 2);

        assertThat(queryForDOI,
                Is.is(IOUtils.toString(getClass().getResourceAsStream("doi-example.sparql"), StandardCharsets.UTF_8))
        );

    }

    @Test
    public void generateQueryUUID() throws IOException {
        String queryForDOI = ProvUtil.generateQuery(
                RefNodeFactory.toIRI("urn:uuid:b15d4952-7d20-46f1-8a3e-556a512b04c5"),
                "uuid",
                "application/dwca",
                RefNodeFactory.toIRI("hash://sha256/5b7fa37bf8b64e7c935c4ff3389e36f8dd162f0705410dd719fd089e1ea253cd"),
                false, 2);

        assertThat(queryForDOI,
                Is.is(IOUtils.toString(getClass().getResourceAsStream("uuid-example.sparql"), StandardCharsets.UTF_8))
        );

    }

    @Test
    public void generateQueryURL() throws IOException {
        String queryForDOI = ProvUtil.generateQuery(
                RefNodeFactory.toIRI("https://ipt.vertnet.org/archive.do?r=msb_mamm"),
                "url",
                "application/dwca",
                RefNodeFactory.toIRI("hash://sha256/5b7fa37bf8b64e7c935c4ff3389e36f8dd162f0705410dd719fd089e1ea253cd"),
                false, 2);

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