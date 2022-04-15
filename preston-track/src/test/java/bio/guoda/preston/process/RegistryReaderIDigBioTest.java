package bio.guoda.preston.process;

import bio.guoda.preston.HashType;
import bio.guoda.preston.Seeds;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.TestUtil;
import bio.guoda.preston.store.TestUtilForProcessor;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDFTerm;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeConstants.WAS_ASSOCIATED_WITH;
import static bio.guoda.preston.RefNodeFactory.toBlank;
import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toLiteral;
import static bio.guoda.preston.RefNodeFactory.toStatement;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.hamcrest.MatcherAssert.assertThat;

public class RegistryReaderIDigBioTest {

    @Test
    public void onSeed() {
        ArrayList<Quad> nodes = new ArrayList<>();
        RegistryReaderIDigBio reader = new RegistryReaderIDigBio(TestUtil.getTestBlobStore(HashType.sha256), TestUtilForProcessor.testListener(nodes));
        RDFTerm bla = toLiteral("bla");
        reader.on(toStatement(Seeds.IDIGBIO, WAS_ASSOCIATED_WITH, bla));
        assertThat(nodes.size(), is(10));
    }

    @Test
    public void onNotSeed() {
        ArrayList<Quad> nodes = new ArrayList<>();
        RegistryReaderIDigBio reader = new RegistryReaderIDigBio(TestUtil.getTestBlobStore(HashType.sha256), TestUtilForProcessor.testListener(nodes));
        RDFTerm bla = toLiteral("bla");
        reader.on(toStatement(Seeds.IDIGBIO, toIRI("https://example.org/bla"), bla));
        assertThat(nodes.size(), is(0));
    }


    @Test
    public void onRegistry() {
        ArrayList<Quad> nodes = new ArrayList<>();
        BlobStoreReadOnly blob = key -> publishersInputStream();
        RegistryReaderIDigBio reader = new RegistryReaderIDigBio(blob, TestUtilForProcessor.testListener(nodes));

        reader.on(toStatement(
                toIRI("https://search.idigbio.org/v2/search/publishers"),
                HAS_VERSION,
                toIRI("http://something")));

        assertThat(nodes.size(), not(is(0)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void onIncompleteRecordSets() {
        ArrayList<Quad> nodes = new ArrayList<>();
        BlobStoreReadOnly blob = key -> incompleteRecordsetInputStream();
        RegistryReaderIDigBio reader = new RegistryReaderIDigBio(blob, TestUtilForProcessor.testListener(nodes));

        reader.on(toStatement(
                toIRI("https://search.idigbio.org/v2/search/recordsets?limit=10000"),
                HAS_VERSION,
                toIRI("http://something")));

        assertThat(nodes.size(), not(is(0)));
    }

    @Test
    public void onCompleteListOfRecordSets() {
        ArrayList<Quad> nodes = new ArrayList<>();
        BlobStoreReadOnly blob = key -> completeRecordsetInputStream();
        RegistryReaderIDigBio reader = new RegistryReaderIDigBio(blob, TestUtilForProcessor.testListener(nodes));

        reader.on(toStatement(
                toIRI("https://search.idigbio.org/v2/search/recordsets?limit=10000"),
                HAS_VERSION,
                toIRI("http://something")));

        assertThat(nodes.size(), is(659));

        assertThat(nodes.get(2).toString(), startsWith("<d2e46893-099f-45eb-9a76-d2a66f43bec8> <http://www.w3.org/ns/prov#hadMember> <http://www.snib.mx/iptconabio/eml.do?r=SNIB-HC022> <"));
        assertThat(nodes.get(3).toString(), startsWith("<http://www.snib.mx/iptconabio/eml.do?r=SNIB-HC022> <http://purl.org/dc/elements/1.1/format> \"application/eml\" <"));
        assertThat(nodes.get(4).toString(), startsWith("<http://www.snib.mx/iptconabio/eml.do?r=SNIB-HC022> <http://purl.org/pav/hasVersion> _:"));

        assertThat(nodes.get(5).toString(), startsWith("<d2e46893-099f-45eb-9a76-d2a66f43bec8> <http://www.w3.org/ns/prov#hadMember> <http://www.snib.mx/iptconabio/archive.do?r=SNIB-HC022> <"));
        assertThat(nodes.get(6).toString(), startsWith("<http://www.snib.mx/iptconabio/archive.do?r=SNIB-HC022> <http://purl.org/dc/elements/1.1/format> \"application/dwca\" <"));
        assertThat(nodes.get(7).toString(), startsWith("<http://www.snib.mx/iptconabio/archive.do?r=SNIB-HC022> <http://purl.org/pav/hasVersion> _:"));

    }

    @Test
    public void onRecordSetView() {
        ArrayList<Quad> nodes = new ArrayList<>();
        BlobStoreReadOnly blob = key -> getClass().getResourceAsStream("idigbio-recordset.json");
        RegistryReaderIDigBio reader = new RegistryReaderIDigBio(blob, TestUtilForProcessor.testListener(nodes));

        reader.on(toStatement(
                toIRI("https://search.idigbio.org/v2/view/recordsets/ba77d411-4179-4dbd-b6c1-39b8a71ae795"),
                HAS_VERSION,
                toIRI("http://something")));

        assertThat(nodes.size(), is(8));

        assertThat(nodes.get(2).toString(), startsWith("<ba77d411-4179-4dbd-b6c1-39b8a71ae795> <http://www.w3.org/ns/prov#hadMember> <http://ipt.vertnet.org:8080/ipt/eml.do?r=uwbm_invertpaleo> <"));
        assertThat(nodes.get(3).toString(), startsWith("<http://ipt.vertnet.org:8080/ipt/eml.do?r=uwbm_invertpaleo> <http://purl.org/dc/elements/1.1/format> \"application/eml\" <"));
        assertThat(nodes.get(4).toString(), startsWith("<http://ipt.vertnet.org:8080/ipt/eml.do?r=uwbm_invertpaleo> <http://purl.org/pav/hasVersion> _:"));

        assertThat(nodes.get(5).toString(), startsWith("<ba77d411-4179-4dbd-b6c1-39b8a71ae795> <http://www.w3.org/ns/prov#hadMember> <http://ipt.vertnet.org:8080/ipt/archive.do?r=uwbm_invertpaleo> <"));
        assertThat(nodes.get(6).toString(), startsWith("<http://ipt.vertnet.org:8080/ipt/archive.do?r=uwbm_invertpaleo> <http://purl.org/dc/elements/1.1/format> \"application/dwca\" <"));
        assertThat(nodes.get(7).toString(), startsWith("<http://ipt.vertnet.org:8080/ipt/archive.do?r=uwbm_invertpaleo> <http://purl.org/pav/hasVersion> _:"));

    }

    @Test
    public void onCompleteListOfRecords() {
        ArrayList<Quad> nodes = new ArrayList<>();
        BlobStoreReadOnly blob = key -> completeRecordsInputStream();
        RegistryReaderIDigBio reader = new RegistryReaderIDigBio(blob, TestUtilForProcessor.testListener(nodes));

        reader.on(toStatement(
                toIRI("https://search.idigbio.org/v2/search/records?limit=10000"),
                HAS_VERSION,
                toIRI("http://something")));

        assertThat(nodes.size(), is(20));

    }

    @Test
    public void onIncompleteListOfRecordsCustomLimit() {
        ArrayList<Quad> nodes = new ArrayList<>();
        BlobStoreReadOnly blob = key -> getClass().getResourceAsStream("idigbio-records-incomplete.json");
        RegistryReaderIDigBio reader = new RegistryReaderIDigBio(blob, TestUtilForProcessor.testListener(nodes));

        reader.on(toStatement(
                toIRI("https://search.idigbio.org/v2/search/records?limit=10"),
                HAS_VERSION,
                toIRI("http://something")));

        assertThat(nodes.size(), is(21));
        assertThat(nodes.get(20).getSubject().ntriplesString(), is("<https://search.idigbio.org/v2/search/records?limit=10&offset=2>"));
    }

    @Test
    public void doNotPageOnIncompleteListOfRecordsWithExplicitOffset() {
        ArrayList<Quad> nodes = new ArrayList<>();
        BlobStoreReadOnly blob = key -> getClass().getResourceAsStream("idigbio-records-incomplete.json");
        RegistryReaderIDigBio reader = new RegistryReaderIDigBio(blob, TestUtilForProcessor.testListener(nodes));

        reader.on(toStatement(
                toIRI("https://search.idigbio.org/v2/search/records?limit=10&offset=2"),
                HAS_VERSION,
                toIRI("http://something")));

        assertThat(nodes.size(), is(20));
    }

    @Test
    public void resolveMediaRecordUUID() {
        final IRI pageIRI = toIRI("https://search.idigbio.org/v2/search/records?limit=10&offset=2");

        final IRI mediaIRI = RegistryReaderIDigBio.resolveMediaUUID(pageIRI, UUID.fromString("45e8135c-5cd9-4424-ae6e-a5910d3f2bb4"));

        assertThat(mediaIRI.getIRIString(), is("https://search.idigbio.org/v2/view/mediarecords/45e8135c-5cd9-4424-ae6e-a5910d3f2bb4"));
    }

    @Test
    public void resolveMediaRecordThumbnail() {
        final IRI pageIRI = toIRI("https://search.idigbio.org/v2/search/records?limit=10&offset=2");

        final IRI mediaIRI = RegistryReaderIDigBio.resolveMediaThumbnail(pageIRI, UUID.fromString("45e8135c-5cd9-4424-ae6e-a5910d3f2bb4"));

        assertThat(mediaIRI.getIRIString(), is("https://api.idigbio.org/v2/media/45e8135c-5cd9-4424-ae6e-a5910d3f2bb4?size=thumbnail"));
    }

    @Test
    public void resolveMediaRecordWebView() {
        final IRI pageIRI = toIRI("https://search.idigbio.org/v2/search/records?limit=10&offset=2");

        final IRI mediaIRI = RegistryReaderIDigBio.resolveMediaWebView(pageIRI, UUID.fromString("45e8135c-5cd9-4424-ae6e-a5910d3f2bb4"));

        assertThat(mediaIRI.getIRIString(), is("https://api.idigbio.org/v2/media/45e8135c-5cd9-4424-ae6e-a5910d3f2bb4?size=webview"));
    }

    @Test
    public void resolveMediaRecordFullSize() {
        final IRI pageIRI = toIRI("https://search.idigbio.org/v2/search/records?limit=10&offset=2");

        final IRI mediaIRI = RegistryReaderIDigBio.resolveMediaFullSize(pageIRI, UUID.fromString("45e8135c-5cd9-4424-ae6e-a5910d3f2bb4"));

        assertThat(mediaIRI.getIRIString(), is("https://api.idigbio.org/v2/media/45e8135c-5cd9-4424-ae6e-a5910d3f2bb4?size=fullsize"));
    }

    @Test
    public void resolveMediaRecordUUIDUnsupportedPageId() {
        final IRI pageIRI = toIRI("https://search.idigbio.org/v2/bla/records?limit=10&offset=2");
        final IRI mediaIRI = RegistryReaderIDigBio.resolveMediaUUID(pageIRI, UUID.fromString("45e8135c-5cd9-4424-ae6e-a5910d3f2bb4"));
        assertThat(mediaIRI, is(nullValue()));
    }

    @Test
    public void onIncompleteListOfRecordsMultiplePages() {
        ArrayList<Quad> nodes = new ArrayList<>();
        BlobStoreReadOnly blob = key -> getClass().getResourceAsStream("idigbio-records-incomplete.json");
        RegistryReaderIDigBio reader = new RegistryReaderIDigBio(blob, TestUtilForProcessor.testListener(nodes));

        reader.on(toStatement(
                toIRI("https://search.idigbio.org/v2/search/records?limit=1"),
                HAS_VERSION,
                toIRI("http://something")));

        final int total = 29;
        assertThat(nodes.size(), is(total));
        assertThat(nodes.get(total - 9).getSubject().ntriplesString(), is("<https://search.idigbio.org/v2/search/records?limit=1&offset=2>"));
        assertThat(nodes.get(total - 1).getSubject().ntriplesString(), is("<https://search.idigbio.org/v2/search/records?limit=1&offset=10>"));
    }

    @Test
    public void parseRecords() throws IOException {

        IRI providedParent = toIRI("someRegistryUUID");
        final List<Quad> nodes = new ArrayList<>();

        InputStream is = completeRecordsInputStream();
        IRI providedPageIRI = toIRI("https://search.something/search/record?foo=bar");

        RegistryReaderIDigBio.parseRecords(providedParent, TestUtilForProcessor.testEmitter(nodes), is, providedPageIRI);

        assertThat(nodes.size(), is(19));

        assertThat(nodes.get(0).toString(), is("<someRegistryUUID> <http://www.w3.org/ns/prov#hadMember> <urn:uuid:e6c5dffc-4ad1-4d9d-800f-5796baec1f65> ."));
        assertThat(nodes.get(2).toString(), is("<urn:uuid:e6c5dffc-4ad1-4d9d-800f-5796baec1f65> <http://www.w3.org/ns/prov#hadMember> <urn:uuid:45e8135c-5cd9-4424-ae6e-a5910d3f2bb4> ."));

        assertThat(nodes.get(3).toString(), startsWith("<https://search.something/view/mediarecords/45e8135c-5cd9-4424-ae6e-a5910d3f2bb4> <http://purl.org/pav/hasVersion> _:"));
        assertThat(nodes.get(4).toString(), startsWith("<https://api.something/media/45e8135c-5cd9-4424-ae6e-a5910d3f2bb4?size=thumbnail> <http://purl.org/pav/hasVersion> _:"));
        assertThat(nodes.get(5).toString(), startsWith("<https://api.something/media/45e8135c-5cd9-4424-ae6e-a5910d3f2bb4?size=webview> <http://purl.org/pav/hasVersion> _:"));
        assertThat(nodes.get(6).toString(), startsWith("<https://api.something/media/45e8135c-5cd9-4424-ae6e-a5910d3f2bb4?size=fullsize> <http://purl.org/pav/hasVersion> _:"));

        assertThat(nodes.get(7).toString(), is("<urn:uuid:e6c5dffc-4ad1-4d9d-800f-5796baec1f65> <http://www.w3.org/ns/prov#hadMember> <urn:uuid:66caac8d-d00c-4d68-9a5c-450e2608d0b5> ."));

        assertThat(nodes.get(8).toString(), startsWith("<https://search.something/view/mediarecords/66caac8d-d00c-4d68-9a5c-450e2608d0b5> <http://purl.org/pav/hasVersion> _:"));

        assertThat(nodes.get(12).toString(), is("<urn:uuid:e6c5dffc-4ad1-4d9d-800f-5796baec1f65> <http://www.w3.org/ns/prov#hadMember> <urn:uuid:00ba0ad7-a11a-4b9f-90b4-299b7949a232> ."));

        assertThat(nodes.get(13).toString(), startsWith("<https://search.something/view/mediarecords/00ba0ad7-a11a-4b9f-90b4-299b7949a232> <http://purl.org/pav/hasVersion> _:"));

        assertThat(nodes.get(17).toString(), is("<someRegistryUUID> <http://www.w3.org/ns/prov#hadMember> <urn:uuid:db16bf3a-550b-4204-92e4-bbc71c96c772> ."));

    }

    @Test
    public void parseMediaRecord() throws IOException {

        IRI providedParent = toIRI("someContentIRI");
        final List<Quad> nodes = new ArrayList<>();

        InputStream is = mediaRecordInputStream();
        IRI providedPageIRI = toIRI("https://something/search/record?foo=bar");

        RegistryReaderIDigBio.parseMediaRecord(providedParent, TestUtilForProcessor.testEmitter(nodes), is, providedPageIRI);

        assertThat(nodes.size(), is(4));

        assertThat(nodes.get(0).toString(), is("<someContentIRI> <http://www.w3.org/ns/prov#hadMember> <urn:uuid:45e8135c-5cd9-4424-ae6e-a5910d3f2bb4> ."));

        assertThat(nodes.get(1).toString(), startsWith("<http://www.burkemuseum.org/research-and-collections/invertebrate-paleontology-and-micropaleontology/collections/database/images/jpeg.php?Image=UWBM_IP_66034_2.jpg> <http://purl.org/pav/hasVersion> _:"));

        assertThat(nodes.get(2).toString(), is("<urn:uuid:45e8135c-5cd9-4424-ae6e-a5910d3f2bb4> <http://rs.tdwg.org/ac/terms/accessURI> <http://www.burkemuseum.org/research-and-collections/invertebrate-paleontology-and-micropaleontology/collections/database/images/jpeg.php?Image=UWBM_IP_66034_2.jpg> ."));

        assertThat(nodes.get(3).toString(), is("<http://www.burkemuseum.org/research-and-collections/invertebrate-paleontology-and-micropaleontology/collections/database/images/jpeg.php?Image=UWBM_IP_66034_2.jpg> <http://xmlns.com/foaf/0.1/depicts> <urn:uuid:e6c5dffc-4ad1-4d9d-800f-5796baec1f65> ."));


    }


    @Test
    public void parsePublishers() throws IOException {

        IRI providedParent = toIRI("someRegistryUUID");
        final List<Quad> nodes = new ArrayList<>();

        InputStream is = publishersInputStream();

        RegistryReaderIDigBio.parsePublishers(providedParent, TestUtilForProcessor.testEmitter(nodes), is);

        assertThat(nodes.size(), is(312));

        Quad node = nodes.get(0);
        assertThat(node.toString(), is("<someRegistryUUID> <http://www.w3.org/ns/prov#hadMember> <urn:uuid:51290816-f682-4e38-a06c-03bf5df2442d> ."));

        node = nodes.get(1);
        assertThat(node.toString(), is("<urn:uuid:51290816-f682-4e38-a06c-03bf5df2442d> <http://www.w3.org/ns/prov#hadMember> <https://www.morphosource.org/rss/ms.rss> ."));

        node = nodes.get(2);
        assertThat(node.toString(), is("<https://www.morphosource.org/rss/ms.rss> <http://purl.org/dc/elements/1.1/format> \"application/rss+xml\" ."));

        node = nodes.get(3);
        assertThat(node.toString(), startsWith("<https://www.morphosource.org/rss/ms.rss> <http://purl.org/pav/hasVersion> "));

        node = nodes.get(4);
        assertThat(node.toString(), is("<someRegistryUUID> <http://www.w3.org/ns/prov#hadMember> <urn:uuid:a9684883-ce9b-4be1-9841-b063fc69e163> ."));

        node = nodes.get(5);
        assertThat(node.toString(), is("<urn:uuid:a9684883-ce9b-4be1-9841-b063fc69e163> <http://www.w3.org/ns/prov#hadMember> <http://portal.torcherbaria.org/portal/webservices/dwc/rss.xml> ."));

        node = nodes.get(6);
        assertThat(node.toString(), is("<http://portal.torcherbaria.org/portal/webservices/dwc/rss.xml> <http://purl.org/dc/elements/1.1/format> \"application/rss+xml\" ."));

        node = nodes.get(7);
        assertThat(node.toString(), startsWith("<http://portal.torcherbaria.org/portal/webservices/dwc/rss.xml> <http://purl.org/pav/hasVersion> "));

        node = nodes.get(8);
        assertThat(node.toString(), is("<someRegistryUUID> <http://www.w3.org/ns/prov#hadMember> <urn:uuid:089a51fa-5f81-48e7-a1b7-9bc539555f29> ."));

    }


    private InputStream publishersInputStream() {
        return getClass().getResourceAsStream("idigbio-publishers.json");
    }

    public InputStream incompleteRecordsetInputStream() {
        return getClass().getResourceAsStream("idigbio-recordsets-incomplete.json");
    }

    public InputStream completeRecordsetInputStream() {
        return getClass().getResourceAsStream("idigbio-recordsets-complete.json");
    }

    public InputStream completeRecordsInputStream() {
        return getClass().getResourceAsStream("idigbio-records-complete.json");
    }

    public InputStream mediaRecordInputStream() {
        return getClass().getResourceAsStream("idigbio-mediarecord.json");
    }

    @Test
    public void isNotRecordSetSearchRequest() {
        final String urlString = "https://search.idigbio.org/v2/view/recordsets/ba77d411-4179-4dbd-b6c1-39b8a71ae795";
        assertFalse(RegistryReaderIDigBio.isRecordSetSearchEndpoint(toIRI(urlString)));
    }

    @Test
    public void isRecordSetSearchRequest() {
        final String urlString = "https://search.idigbio.org/v2/search/recordsets";
        assertTrue(RegistryReaderIDigBio.isRecordSetSearchEndpoint(toIRI(urlString)));
    }

    @Test
    public void isRecordSetViewRequest() {
        final String urlString = "https://search.idigbio.org/v2/view/recordsets/ba77d411-4179-4dbd-b6c1-39b8a71ae795";
        assertTrue(RegistryReaderIDigBio.isRecordSetViewEndpoint(toIRI(urlString)));
    }

    @Test
    public void isNotRecordSetViewRequest() {
        final String urlString = "https://search.idigbio.org/v2/search/recordsets";
        assertFalse(RegistryReaderIDigBio.isRecordSetViewEndpoint(toIRI(urlString)));
    }

    @Test
    public void emitNonCachedIRI() {
        final ArrayList<Quad> nodes = new ArrayList<>();
        final StatementsEmitter cachingEmitter = RegistryReaderIDigBio.createCachingEmitter(nodes, new HashSet<>());
        final Quad quad = RefNodeFactory.toStatement(RefNodeFactory.toIRI("https://example.org/"), HAS_VERSION, toBlank());
        cachingEmitter.emit(Collections.singletonList(quad));
        cachingEmitter.emit(Collections.singletonList(quad));
        assertThat(nodes.size(), is(2));
    }

    @Test
    public void doNotEmitNonCachedIRI() {
        final ArrayList<Quad> statements = new ArrayList<>();
        final StatementsEmitter cachingEmitter = RegistryReaderIDigBio.createCachingEmitter(statements, new HashSet<>());
        final Quad quad = RefNodeFactory.toStatement(RefNodeFactory.toIRI("https://search.idigbio.org/v2/view/recordsets/ba77d411-4179-4dbd-b6c1-39b8a71ae795"), HAS_VERSION, toBlank());
        cachingEmitter.emit(Collections.singletonList(quad));
        cachingEmitter.emit(Collections.singletonList(quad));
        assertThat(statements.size(), is(1));
    }

}