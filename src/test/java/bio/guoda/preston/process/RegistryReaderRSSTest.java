package bio.guoda.preston.process;

import bio.guoda.preston.model.RefNodeFactory;
import bio.guoda.preston.store.KeyValueStoreReadOnly;
import bio.guoda.preston.store.TestUtil;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class RegistryReaderRSSTest {

    @Test
    public void onNotRSSVersion() {
        ArrayList<Quad> nodes = new ArrayList<>();
        StatementsListener registryReader = new RegistryReaderRSS(TestUtil.getTestBlobStore(), TestUtil.testListener(nodes));

        registryReader.on(toStatement(toIRI("donaldduck"), HAS_VERSION, toIRI("somehash")));
        assertThat(nodes.size(), is(0));
    }

    @Test
    public void onRSSVersion() {
        ArrayList<Quad> nodes = new ArrayList<>();
        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) throws IOException {
                assertThat(key, is(toIRI("somehash")));
                return getClass().getResourceAsStream("vertnet-ipt-rss.xml");
            }
        };
        StatementsListener registryReader = new RegistryReaderRSS(blobStore, TestUtil.testListener(nodes));

        registryReader.on(toStatement(toIRI("daisyduck"), HAS_VERSION, toIRI("somehash")));
        assertThat(nodes.size(), is(1850));
    }

    @Test
    public void parseFeeds() {
        IRI parent = toIRI("http://example.org");
        List<Quad> nodes = new ArrayList<>();
        StatementsEmitter emitter = TestUtil.testEmitter(nodes);

        KeyValueStoreReadOnly readOnlyStore = (IRI key) -> getClass().getResourceAsStream("torch-portal-rss.xml");

        RegistryReaderRSS.parse(parent, emitter, readOnlyStore);

        assertThat(nodes.size(), is(28));

        List<String> actual = nodes.stream().limit(3).map(Object::toString).collect(Collectors.toList());

        List<String> expected = Arrays.asList("<http://example.org> <http://www.w3.org/ns/prov#hadMember> <fea81a47-2365-45cc-bef9-b6bbff7457e6> .",
                "<fea81a47-2365-45cc-bef9-b6bbff7457e6> <http://www.w3.org/ns/prov#hadMember> <http://portal.torcherbaria.org/portal/content/dwca/BRIT_DwC-A.eml> .",
                "<http://portal.torcherbaria.org/portal/content/dwca/BRIT_DwC-A.eml> <http://purl.org/dc/elements/1.1/format> \"application/eml\" .");
        assertThat(actual.size(), is(expected.size()));

        for (int i = 0; i < expected.size(); i++) {
            assertThat(expected.get(i), is(actual.get(i)));
        }
        assertThat(actual, is(expected));
    }


    @Test
    public void parseArthopodEasyFeeds() {
        IRI parent = RefNodeFactory.toIRI("http://example.org");
        List<Quad> nodes = new ArrayList<>();
        StatementsEmitter emitter = TestUtil.testEmitter(nodes);
        KeyValueStoreReadOnly readOnlyStore = (IRI key) -> getClass().getResourceAsStream("arthropodEasyCapture.xml");

        RegistryReaderRSS.parse(parent, emitter, readOnlyStore);

        assertThat(nodes.size(), is(21));

        List<String> actual = nodes.stream().limit(3).map(Object::toString).collect(Collectors.toList());

        List<String> expected = Arrays.asList(
                "<http://example.org> <http://www.w3.org/ns/prov#hadMember> <urn:uuid:f0cec69a-853c-11e4-8259-0026552be7ea> .",
                "<urn:uuid:f0cec69a-853c-11e4-8259-0026552be7ea> <http://www.w3.org/ns/prov#hadMember> <http://amnh.begoniasociety.org/dwc/AEC-TTD-TCN_DwC-A20160308.eml> .",
                "<http://amnh.begoniasociety.org/dwc/AEC-TTD-TCN_DwC-A20160308.eml> <http://purl.org/dc/elements/1.1/format> \"application/eml\" ."
        );

        assertThat(actual, is(expected));

        actual = nodes.stream().skip(4).limit(2).map(Object::toString).collect(Collectors.toList());

        expected = Arrays.asList(
                "<urn:uuid:f0cec69a-853c-11e4-8259-0026552be7ea> <http://www.w3.org/ns/prov#hadMember> <http://amnh.begoniasociety.org/dwc/AEC-TTD-TCN_DwC-A20160308.zip> .",
                "<http://amnh.begoniasociety.org/dwc/AEC-TTD-TCN_DwC-A20160308.zip> <http://purl.org/dc/elements/1.1/format> \"application/dwca\" ."
        );

        assertThat(actual, is(expected));
    }



    @Test
    public void parseSymbiotaFeeds() throws IOException {
        IRI parent = RefNodeFactory.toIRI("http://example.org");
        List<Quad> nodes = new ArrayList<>();
        StatementsEmitter emitter = TestUtil.testEmitter(nodes);


        RegistryReaderRSS.parse(parent, emitter, (IRI key) -> getClass().getResourceAsStream("symbiota-rss.xml"));

        assertThat(nodes.size(), is(189));

        List<String> labels = nodes.stream().limit(9).map(Object::toString).collect(Collectors.toList());

        assertThat(labels.get(0), is("<http://example.org> <http://www.w3.org/ns/prov#hadMember> <4b9c73cc-d12d-4654-bdfb-081dce21729b> ."));
        assertThat(labels.get(1), is(
                "<4b9c73cc-d12d-4654-bdfb-081dce21729b> <http://www.w3.org/ns/prov#hadMember> <http://midwestherbaria.org/portal/content/dwca/ALBC_DwC-A.eml> ."));
        assertThat(labels.get(2), is(
                "<http://midwestherbaria.org/portal/content/dwca/ALBC_DwC-A.eml> <http://purl.org/dc/elements/1.1/format> \"application/eml\" ."));

        assertThat(labels.get(3), startsWith("<http://midwestherbaria.org/portal/content/dwca/ALBC_DwC-A.eml> <http://purl.org/pav/hasVersion>"));

        assertThat(labels.get(5), is(
                "<http://midwestherbaria.org/portal/content/dwca/ALBC_DwC-A.zip> <http://purl.org/dc/elements/1.1/format> \"application/dwca\" ."));

        assertThat(labels.get(4), is(
                "<4b9c73cc-d12d-4654-bdfb-081dce21729b> <http://www.w3.org/ns/prov#hadMember> <http://midwestherbaria.org/portal/content/dwca/ALBC_DwC-A.zip> ."));

        assertThat(labels.get(6), startsWith("<http://midwestherbaria.org/portal/content/dwca/ALBC_DwC-A.zip> <http://purl.org/pav/hasVersion>"));

        assertThat(labels.get(7), is("<http://example.org> <http://www.w3.org/ns/prov#hadMember> <b01789b2-c5d7-11e4-b6af-00163e00498d> ."));

    }

    @Test
    public void parseIntermountainFeeds() throws IOException {
        IRI parent = RefNodeFactory.toIRI("http://example.org");
        List<Quad> nodes = new ArrayList<>();
        StatementsEmitter emitter = TestUtil.testEmitter(nodes);

        RegistryReaderRSS.parse(parent, emitter, (IRI key) -> getClass().getResourceAsStream("intermountain-biota-rss.xml"));

        assertThat(nodes.size(), is(84));
    }

    @Test
    public void parseIPTRSS() throws IOException {
        IRI parent = RefNodeFactory.toIRI("http://example.org");
        List<Quad> nodes = new ArrayList<>();
        StatementsEmitter emitter = TestUtil.testEmitter(nodes);

        RegistryReaderRSS.parse(parent, emitter, (IRI key) -> getClass().getResourceAsStream("ipt-norway-rss.xml"));

        boolean hasEML = false;
        boolean hasEMLLink = false;
        boolean hasDWCA = false;
        boolean hasDWCALink = false;
        for (Quad node : nodes) {
            hasEML = hasEML || node.getObject().toString().equals("\"application/eml\"");
            hasDWCA = hasDWCA || node.getObject().toString().equals("\"application/dwca\"");
            hasEMLLink = hasEMLLink || node.getObject().toString().equals("<https://data.gbif.no/ipt/eml.do?r=ethopia-trees>");
            hasDWCALink = hasDWCALink || node.getObject().toString().equals("<https://data.gbif.no/ipt/archive.do?r=ethopia-trees>");
        }

        assertTrue(hasEML);
        assertTrue(hasEMLLink);
        assertTrue(hasDWCA);
        assertTrue(hasDWCALink);
        assertThat(nodes.size(), is(1128));
    }

    @Test
    public void parseVertNetIPTRSS() throws IOException {
        IRI parent = RefNodeFactory.toIRI("http://example.org");
        List<Quad> nodes = new ArrayList<>();
        StatementsEmitter emitter = TestUtil.testEmitter(nodes);

        RegistryReaderRSS.parse(parent, emitter, (IRI key) -> getClass().getResourceAsStream("vertnet-ipt-rss.xml"));

        boolean hasEML = false;
        boolean hasEMLLink = false;
        boolean hasDWCA = false;
        boolean hasDWCALink = false;
        for (Quad node : nodes) {
            hasEML = hasEML || node.getObject().toString().equals("\"application/eml\"");
            hasDWCA = hasDWCA || node.getObject().toString().equals("\"application/dwca\"");
            hasEMLLink = hasEMLLink || node.getObject().toString().equals("<http://ipt.vertnet.org:8080/ipt/eml.do?r=ucm_egg>");
            hasDWCALink = hasDWCALink || node.getObject().toString().equals("<http://ipt.vertnet.org:8080/ipt/archive.do?r=ucm_egg>");
        }

        assertTrue(hasEML);
        assertTrue(hasEMLLink);
        assertTrue(hasDWCA);
        assertTrue(hasDWCALink);
        assertThat(nodes.size(), is(1849));
    }


}