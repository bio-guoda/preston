package org.globalbioticinteractions.preston.process;

import com.sun.syndication.io.FeedException;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDFTerm;
import org.apache.commons.rdf.api.Triple;
import org.globalbioticinteractions.preston.Seeds;
import org.globalbioticinteractions.preston.model.RefNodeFactory;
import org.globalbioticinteractions.preston.store.TestUtil;
import org.junit.Test;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLStreamException;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.globalbioticinteractions.preston.RefNodeConstants.*;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class RegistryReaderIDigBioTest {

    @Test
    public void onSeed() {
        ArrayList<Triple> nodes = new ArrayList<>();
        RegistryReaderIDigBio reader = new RegistryReaderIDigBio(TestUtil.getTestBlobStore(), nodes::add);
        RDFTerm bla = RefNodeFactory.toLiteral("bla");
        reader.on(RefNodeFactory.toStatement(Seeds.IDIGBIO, WAS_ASSOCIATED_WITH, bla));
        assertThat(nodes.size(), is(5));
    }

    @Test
    public void onNotSeed() {
        ArrayList<Triple> nodes = new ArrayList<>();
        RegistryReaderIDigBio reader = new RegistryReaderIDigBio(TestUtil.getTestBlobStore(), nodes::add);
        RDFTerm bla = RefNodeFactory.toLiteral("bla");
        reader.on(RefNodeFactory.toStatement(Seeds.IDIGBIO, RefNodeFactory.toIRI("https://example.org/bla"), bla));
        assertThat(nodes.size(), is(0));
    }

    @Test
    public void parsePublishers() throws IOException {

        IRI providedParent = RefNodeFactory.toUUID("someRegistryUUID");
        final List<Triple> nodes = new ArrayList<>();

        InputStream is = getClass().getResourceAsStream("idigbio-publishers.json");

        RegistryReaderIDigBio.parsePublishers(providedParent, nodes::add, is);

        assertThat(nodes.size(), is(312));

        Triple node = nodes.get(0);
        assertThat(node.toString(), is("<someRegistryUUID> <http://www.w3.org/ns/prov#hadMember> <51290816-f682-4e38-a06c-03bf5df2442d> ."));

        node = nodes.get(1);
        assertThat(node.toString(), is("<51290816-f682-4e38-a06c-03bf5df2442d> <http://www.w3.org/ns/prov#hadMember> <https://www.morphosource.org/rss/ms.rss> ."));

        node = nodes.get(2);
        assertThat(node.toString(), is("<https://www.morphosource.org/rss/ms.rss> <http://purl.org/dc/elements/1.1/format> \"application/rss+xml\" ."));

        node = nodes.get(3);
        assertThat(node.toString(), startsWith("<https://www.morphosource.org/rss/ms.rss> <http://purl.org/pav/hasVersion> "));

        node = nodes.get(4);
        assertThat(node.toString(), is("<someRegistryUUID> <http://www.w3.org/ns/prov#hadMember> <a9684883-ce9b-4be1-9841-b063fc69e163> ."));

        node = nodes.get(5);
        assertThat(node.toString(), is("<a9684883-ce9b-4be1-9841-b063fc69e163> <http://www.w3.org/ns/prov#hadMember> <http://portal.torcherbaria.org/portal/webservices/dwc/rss.xml> ."));

        node = nodes.get(6);
        assertThat(node.toString(), is("<http://portal.torcherbaria.org/portal/webservices/dwc/rss.xml> <http://purl.org/dc/elements/1.1/format> \"application/rss+xml\" ."));

        node = nodes.get(7);
        assertThat(node.toString(), startsWith("<http://portal.torcherbaria.org/portal/webservices/dwc/rss.xml> <http://purl.org/pav/hasVersion> "));

        node = nodes.get(8);
        assertThat(node.toString(), is("<someRegistryUUID> <http://www.w3.org/ns/prov#hadMember> <089a51fa-5f81-48e7-a1b7-9bc539555f29> ."));

    }

    @Test
    public void parseFeeds() throws XMLStreamException, IOException, FeedException, ParserConfigurationException, SAXException, XPathExpressionException {
        IRI parent = RefNodeFactory.toIRI("http://example.org");
        List<Triple> nodes = new ArrayList<>();
        StatementEmitter emitter = nodes::add;
        InputStream is = getClass().getResourceAsStream("torch-portal-rss.xml");

        RegistryReaderIDigBio.parseRssFeed(parent, emitter, is);

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
    public void parseSymbiotaFeeds() throws XMLStreamException, IOException, FeedException, ParserConfigurationException, SAXException, XPathExpressionException {
        IRI parent = RefNodeFactory.toIRI("http://example.org");
        List<Triple> nodes = new ArrayList<>();
        StatementEmitter emitter = nodes::add;
        InputStream is = getClass().getResourceAsStream("symbiota-rss.xml");

        RegistryReaderIDigBio.parseRssFeed(parent, emitter, is);

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
    public void parseIntermountainFeeds() throws XMLStreamException, IOException, FeedException, ParserConfigurationException, SAXException, XPathExpressionException {
        IRI parent = RefNodeFactory.toIRI("http://example.org");
        List<Triple> nodes = new ArrayList<>();
        StatementEmitter emitter = nodes::add;
        InputStream is = getClass().getResourceAsStream("intermountain-biota-rss.xml");

        RegistryReaderIDigBio.parseRssFeed(parent, emitter, is);

        assertThat(nodes.size(), is(84));
    }


}