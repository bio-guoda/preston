package bio.guoda.preston.server;

import bio.guoda.preston.MimeTypes;
import bio.guoda.preston.RefNodeFactory;
import org.apache.commons.rdf.api.IRI;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Map;

import static bio.guoda.preston.server.RedirectingServlet.ACTIVITY;
import static bio.guoda.preston.server.RedirectingServlet.ARCHIVE_URL;
import static bio.guoda.preston.server.RedirectingServlet.CONTENT_ID;
import static bio.guoda.preston.server.RedirectingServlet.CONTENT_TYPE;
import static bio.guoda.preston.server.RedirectingServlet.DOI;
import static bio.guoda.preston.server.RedirectingServlet.PROVENANCE_ID;
import static bio.guoda.preston.server.RedirectingServlet.SEEN_AT;
import static bio.guoda.preston.server.RedirectingServlet.UUID;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;

public class RedirectingServletIT {

    public static final IRI PROVENANCE_ID_VALUE = RefNodeFactory.toIRI("hash://sha256/5b7fa37bf8b64e7c935c4ff3389e36f8dd162f0705410dd719fd089e1ea253cd");
    private String sparqlEndpoint = "http://localhost:7878/query";

    @Test
    public void dealiasDOI() throws IOException, URISyntaxException {
        Map<String, String> contentId = ProvUtil.findMostRecentContentId(
                RefNodeFactory.toIRI("https://doi.org/10.15468/aomfnb"),
                ProvUtil.QUERY_TYPE_DOI,
                sparqlEndpoint,
                MimeTypes.MIME_TYPE_DWCA,
                RefNodeFactory.toIRI("hash://sha256/5b7fa37bf8b64e7c935c4ff3389e36f8dd162f0705410dd719fd089e1ea253cd")
        );
        assertThat(contentId.get(CONTENT_ID), Is.is("hash://sha256/1e2b7436fce1848f41698e5a9c193f311abaf0ee051bec1a2e48b5106d29524d"));
        assertThat(contentId.get(ACTIVITY), Is.is("urn:uuid:d8bdd083-997a-44c5-825a-bb0fe4e491dd"));
        assertThat(contentId.get(CONTENT_TYPE), Is.is("application/dwca"));
    }

    @Test
    public void dealiasUUIDGBIF() throws IOException, URISyntaxException {
        Map<String, String> contentId = ProvUtil.findMostRecentContentId(
                RefNodeFactory.toIRI("urn:uuid:4fa7b334-ce0d-4e88-aaae-2e0c138d049e"),
                ProvUtil.QUERY_TYPE_UUID,
                sparqlEndpoint,
                MimeTypes.MIME_TYPE_DWCA,
                RefNodeFactory.toIRI("hash://sha256/5b7fa37bf8b64e7c935c4ff3389e36f8dd162f0705410dd719fd089e1ea253cd"));
        assertThat(contentId.get(CONTENT_ID), Is.is("hash://sha256/1e2b7436fce1848f41698e5a9c193f311abaf0ee051bec1a2e48b5106d29524d"));
        assertThat(contentId.get(CONTENT_TYPE), Is.is("application/dwca"));
    }

    @Test
    public void dealiasUUID_GBIF_EML() throws IOException, URISyntaxException {
        Map<String, String> contentId = ProvUtil.findMostRecentContentId(
                RefNodeFactory.toIRI("urn:uuid:a44859c6-af4f-4a2a-a184-1b2d68c82099"),
                ProvUtil.QUERY_TYPE_UUID,
                sparqlEndpoint,
                MimeTypes.MIME_TYPE_EML,
                RefNodeFactory.toIRI("hash://sha256/5b7fa37bf8b64e7c935c4ff3389e36f8dd162f0705410dd719fd089e1ea253cd"));
        assertThat(contentId.get(CONTENT_ID), Is.is("hash://sha256/0e07014e3835de619d4aa658c2eb11b1cba74716769d4884018c3b21e4f3d7c5"));
        assertThat(contentId.get(CONTENT_TYPE), Is.is("application/eml"));
    }

    @Test
    public void dealiasUUID_iDigBio_EML() throws IOException, URISyntaxException {
        Map<String, String> contentId = ProvUtil.findMostRecentContentId(
                RefNodeFactory.toIRI("urn:uuid:c9316f11-d955-4472-a276-6a26a6514590"),
                ProvUtil.QUERY_TYPE_UUID,
                sparqlEndpoint,
                MimeTypes.MIME_TYPE_EML,
                RefNodeFactory.toIRI("hash://sha256/5b7fa37bf8b64e7c935c4ff3389e36f8dd162f0705410dd719fd089e1ea253cd"));
        assertThat(contentId.get(CONTENT_ID), Is.is("hash://sha256/23e74c5513ef391a7e757643d20970b44633cc2f768f926c815507d804ae3cb5"));
        assertThat(contentId.get(CONTENT_TYPE), Is.is("application/eml"));
    }

    @Test
    public void dealiasContentIdGBIF() throws IOException, URISyntaxException {
        Map<String, String> contentId = ProvUtil.findMostRecentContentId(
                RefNodeFactory.toIRI("hash://sha256/1e2b7436fce1848f41698e5a9c193f311abaf0ee051bec1a2e48b5106d29524d"),
                ProvUtil.QUERY_TYPE_CONTENT_ID,
                sparqlEndpoint,
                MimeTypes.MIME_TYPE_DWCA,
                RefNodeFactory.toIRI("hash://sha256/5b7fa37bf8b64e7c935c4ff3389e36f8dd162f0705410dd719fd089e1ea253cd"));
        assertThat(contentId.get(CONTENT_ID), Is.is("hash://sha256/1e2b7436fce1848f41698e5a9c193f311abaf0ee051bec1a2e48b5106d29524d"));
        assertThat(contentId.get(CONTENT_TYPE), Is.is("application/dwca"));
        assertThat(contentId.get(ACTIVITY), Is.is("urn:uuid:d8bdd083-997a-44c5-825a-bb0fe4e491dd"));
        assertThat(contentId.get(PROVENANCE_ID), Is.is("hash://sha256/5b7fa37bf8b64e7c935c4ff3389e36f8dd162f0705410dd719fd089e1ea253cd"));
        assertThat(contentId.get(SEEN_AT), Is.is("2024-01-02T17:28:41.198Z"));
        assertThat(contentId.get(ARCHIVE_URL), Is.is("https://hosted-datasets.gbif.org/eBird/2022-eBird-dwca-1.0.zip"));
        assertThat(contentId.get(UUID), Is.is("urn:uuid:4fa7b334-ce0d-4e88-aaae-2e0c138d049e"));
        assertThat(contentId.get(DOI), Is.is("https://doi.org/10.15468/aomfnb"));
    }

    @Test
    public void dealiasContentIdiDigBio() throws IOException, URISyntaxException {
        Map<String, String> contentId = ProvUtil.findMostRecentContentId(
                RefNodeFactory.toIRI("hash://sha256/f5d8f67c1eca34cbba1abac12f353585c78bb053bc8ce7ee7e7a78846e1bfc4a"),
                ProvUtil.QUERY_TYPE_CONTENT_ID,
                sparqlEndpoint,
                MimeTypes.MIME_TYPE_DWCA,
                PROVENANCE_ID_VALUE);
        assertThat(contentId.get(CONTENT_ID), Is.is("hash://sha256/f5d8f67c1eca34cbba1abac12f353585c78bb053bc8ce7ee7e7a78846e1bfc4a"));
        assertThat(contentId.get(CONTENT_TYPE), Is.is("application/dwca"));
        assertThat(contentId.get(ACTIVITY), Is.is("urn:uuid:603cb45b-c23e-4d3e-a0bf-604d8537296d"));
        assertThat(contentId.get(PROVENANCE_ID), Is.is("hash://sha256/5b7fa37bf8b64e7c935c4ff3389e36f8dd162f0705410dd719fd089e1ea253cd"));
        assertThat(contentId.get(SEEN_AT), Is.is("2023-12-03T06:16:07.462Z"));
        assertThat(contentId.get(ARCHIVE_URL), Is.is("https://ecdysis.org/content/dwca/UCSB-IZC_DwC-A.zip"));
        assertThat(contentId.get(UUID), Is.is("urn:uuid:d6097f75-f99e-4c2a-b8a5-b0fc213ecbd0"));
        assertThat(contentId.get(DOI), Is.is("https://doi.org/10.15468/w6hvhv"));
    }

    @Test
    public void dealiasUUIDiDigBio() throws IOException, URISyntaxException {
        Map<String, String> contentId = ProvUtil.findMostRecentContentId(
                RefNodeFactory.toIRI("urn:uuid:65007e62-740c-4302-ba20-260fe68da291"),
                ProvUtil.QUERY_TYPE_UUID,
                sparqlEndpoint,
                MimeTypes.MIME_TYPE_DWCA,
                RefNodeFactory.toIRI("hash://sha256/5b7fa37bf8b64e7c935c4ff3389e36f8dd162f0705410dd719fd089e1ea253cd"));
        assertThat(contentId.get(CONTENT_ID), Is.is("hash://sha256/641b73fbee95c3965e66e2b65630ba2fdb0af6786b9cae2d08d8f03089fc4c35"));
        assertThat(contentId.get(CONTENT_TYPE), Is.is("application/dwca"));

    }

    @Test
    public void dealiasUUIDiDigBioEML() throws IOException, URISyntaxException {
        Map<String, String> contentId = ProvUtil.findMostRecentContentId(
                RefNodeFactory.toIRI("urn:uuid:65007e62-740c-4302-ba20-260fe68da291"),
                ProvUtil.QUERY_TYPE_UUID,
                sparqlEndpoint,
                MimeTypes.MIME_TYPE_EML,
                RefNodeFactory.toIRI("hash://sha256/5b7fa37bf8b64e7c935c4ff3389e36f8dd162f0705410dd719fd089e1ea253cd"));
        assertThat(contentId.get(CONTENT_ID), Is.is("hash://sha256/7d096e34c66750624036f4fe13bca597c7f0cec1c3ff4347c3175c529001ace1"));
        assertThat(contentId.get(CONTENT_TYPE), Is.is("application/eml"));

    }

    @Test
    public void dealiasURL() throws URISyntaxException, IOException {
        Map<String, String> contentId = ProvUtil.findMostRecentContentId(
                RefNodeFactory.toIRI("https://hosted-datasets.gbif.org/eBird/2022-eBird-dwca-1.0.zip"),
                ProvUtil.QUERY_TYPE_URL,
                sparqlEndpoint,
                MimeTypes.MIME_TYPE_DWCA,
                RefNodeFactory.toIRI("hash://sha256/5b7fa37bf8b64e7c935c4ff3389e36f8dd162f0705410dd719fd089e1ea253cd"));
        assertThat(contentId.get(CONTENT_ID), Is.is("hash://sha256/1e2b7436fce1848f41698e5a9c193f311abaf0ee051bec1a2e48b5106d29524d"));
        assertThat(contentId.get(CONTENT_TYPE), Is.is("application/dwca"));
    }

    @Test
    public void dealiasURLNoAccess() throws URISyntaxException, IOException {
        Map<String, String> contentId = ProvUtil.findMostRecentContentId(
                RefNodeFactory.toIRI("https://registry.nbnatlas.org/archives/dr940/dr940.zip"),
                ProvUtil.QUERY_TYPE_URL,
                sparqlEndpoint,
                MimeTypes.MIME_TYPE_DWCA,
                RefNodeFactory.toIRI("hash://sha256/5b7fa37bf8b64e7c935c4ff3389e36f8dd162f0705410dd719fd089e1ea253cd"));
        assertThat(contentId.get(CONTENT_ID), startsWith("https://deeplinker.bio/.well-known/genid"));
        assertThat(contentId.get(CONTENT_TYPE), Is.is("application/dwca"));
        assertThat(contentId.get(DOI), Is.is("https://doi.org/10.15468/mwjnku"));
        assertThat(contentId.get(UUID), Is.is("urn:uuid:926f5a1c-8995-498a-913b-fe0312e1071f"));
        assertThat(contentId.get(SEEN_AT), Is.is("2024-01-03T06:13:59.079Z"));
    }

    @Test
    public void dealiasUUIDNoAccess() throws URISyntaxException, IOException {
        Map<String, String> contentId = ProvUtil.findMostRecentContentId(
                RefNodeFactory.toIRI("urn:uuid:926f5a1c-8995-498a-913b-fe0312e1071f"),
                ProvUtil.QUERY_TYPE_UUID,
                sparqlEndpoint,
                MimeTypes.MIME_TYPE_DWCA,
                RefNodeFactory.toIRI("hash://sha256/5b7fa37bf8b64e7c935c4ff3389e36f8dd162f0705410dd719fd089e1ea253cd"));
        assertThat(contentId.get(CONTENT_ID), startsWith("https://deeplinker.bio/.well-known/genid"));
        assertThat(contentId.get(CONTENT_TYPE), Is.is("application/dwca"));
        assertThat(contentId.get(DOI), Is.is("https://doi.org/10.15468/mwjnku"));
        assertThat(contentId.get(UUID), Is.is("urn:uuid:926f5a1c-8995-498a-913b-fe0312e1071f"));
        assertThat(contentId.get(SEEN_AT), Is.is("2024-01-03T06:13:59.079Z"));
    }

    @Test
    public void dealiasDOINoAccess() throws URISyntaxException, IOException {
        Map<String, String> contentId = ProvUtil.findMostRecentContentId(
                RefNodeFactory.toIRI("https://doi.org/10.15468/mwjnku"),
                ProvUtil.QUERY_TYPE_DOI,
                sparqlEndpoint,
                MimeTypes.MIME_TYPE_DWCA,
                RefNodeFactory.toIRI("hash://sha256/5b7fa37bf8b64e7c935c4ff3389e36f8dd162f0705410dd719fd089e1ea253cd"));
        assertThat(contentId.get(CONTENT_ID), startsWith("https://deeplinker.bio/.well-known/genid"));
        assertThat(contentId.get(CONTENT_TYPE), Is.is("application/dwca"));
        assertThat(contentId.get(DOI), Is.is("https://doi.org/10.15468/mwjnku"));
        assertThat(contentId.get(UUID), Is.is("urn:uuid:926f5a1c-8995-498a-913b-fe0312e1071f"));
        assertThat(contentId.get(SEEN_AT), Is.is("2024-01-03T06:13:59.079Z"));
    }

    @Test
    public void dealias_GBIF_URL() throws URISyntaxException, IOException {
        Map<String, String> contentId = ProvUtil.findMostRecentContentId(
                RefNodeFactory.toIRI("http://gbif.ru:8080/ipt/eml.do?r=kurilfauna"),
                ProvUtil.QUERY_TYPE_URL,
                sparqlEndpoint,
                MimeTypes.MIME_TYPE_EML,
                RefNodeFactory.toIRI("hash://sha256/5b7fa37bf8b64e7c935c4ff3389e36f8dd162f0705410dd719fd089e1ea253cd"));
        assertThat(contentId.get(CONTENT_ID), Is.is("hash://sha256/0e07014e3835de619d4aa658c2eb11b1cba74716769d4884018c3b21e4f3d7c5"));
        assertThat(contentId.get(CONTENT_TYPE), Is.is("application/eml"));
    }

    @Test
    public void dealias_iDigBio_URL() throws URISyntaxException, IOException {
        Map<String, String> contentId = ProvUtil.findMostRecentContentId(
                RefNodeFactory.toIRI("http://ipt.vertnet.org:8080/ipt/eml.do?r=lsumz_fishes"),
                ProvUtil.QUERY_TYPE_URL,
                sparqlEndpoint,
                MimeTypes.MIME_TYPE_EML,
                RefNodeFactory.toIRI("hash://sha256/5b7fa37bf8b64e7c935c4ff3389e36f8dd162f0705410dd719fd089e1ea253cd"));
        assertThat(contentId.get(CONTENT_ID), Is.is("hash://sha256/c75b1864de7c906ab0685fce5c2eba44ed62b11aee9b0aa15081a9c5c90b32b5"));
        assertThat(contentId.get(CONTENT_TYPE), Is.is("application/eml"));
    }

}