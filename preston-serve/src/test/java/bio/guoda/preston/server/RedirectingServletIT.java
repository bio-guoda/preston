package bio.guoda.preston.server;

import bio.guoda.preston.RefNodeFactory;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Map;

import static bio.guoda.preston.server.RedirectingServlet.ACTIVITY;
import static bio.guoda.preston.server.RedirectingServlet.CONTENT_ID;
import static org.hamcrest.MatcherAssert.assertThat;

public class RedirectingServletIT {


    private String sparqlEndpoint = "http://localhost:7878/query";

    @Test
    public void dealiasDOI() throws IOException, URISyntaxException {
        Map<String, String> contentId = ProvUtil.findMostRecentContentId(
                RefNodeFactory.toIRI("https://doi.org/10.15468/aomfnb"),
                ProvUtil.QUERY_TYPE_DOI,
                sparqlEndpoint
        );
        assertThat(contentId.get(CONTENT_ID), Is.is("hash://sha256/1e2b7436fce1848f41698e5a9c193f311abaf0ee051bec1a2e48b5106d29524d"));
        assertThat(contentId.get(ACTIVITY), Is.is("urn:uuid:77f3faf7-acd2-4f14-9c0e-4e04ef5b63c7"));
    }

    @Test
    public void dealiasUUIDGBIF() throws IOException, URISyntaxException {
        Map<String, String> contentId = ProvUtil.findMostRecentContentId(
                RefNodeFactory.toIRI("urn:uuid:4fa7b334-ce0d-4e88-aaae-2e0c138d049e"),
                ProvUtil.QUERY_TYPE_UUID,
                sparqlEndpoint);
        assertThat(contentId.get(CONTENT_ID), Is.is("hash://sha256/1e2b7436fce1848f41698e5a9c193f311abaf0ee051bec1a2e48b5106d29524d"));

    }

    @Test
    public void dealiasUUIDiDigBio() throws IOException, URISyntaxException {
        Map<String, String> contentId = ProvUtil.findMostRecentContentId(
                RefNodeFactory.toIRI("urn:uuid:65007e62-740c-4302-ba20-260fe68da291"),
                ProvUtil.QUERY_TYPE_UUID,
                sparqlEndpoint);
        assertThat(contentId.get(CONTENT_ID), Is.is("hash://sha256/f5d8f67c1eca34cbba1abac12f353585c78bb053bc8ce7ee7e7a78846e1bfc4a"));

    }

    @Test
    public void dealiasURL() throws URISyntaxException, IOException {
        Map<String, String> contentId = ProvUtil.findMostRecentContentId(
                RefNodeFactory.toIRI("https://hosted-datasets.gbif.org/eBird/2022-eBird-dwca-1.0.zip"),
                ProvUtil.QUERY_TYPE_URL,
                sparqlEndpoint);
        assertThat(contentId.get(CONTENT_ID), Is.is("hash://sha256/1e2b7436fce1848f41698e5a9c193f311abaf0ee051bec1a2e48b5106d29524d"));
    }

}