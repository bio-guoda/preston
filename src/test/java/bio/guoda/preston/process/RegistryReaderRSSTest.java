package bio.guoda.preston.process;

import bio.guoda.preston.model.RefNodeFactory;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Triple;
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

import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class RegistryReaderRSSTest {

    @Test
    public void parseFeeds() throws XMLStreamException, IOException, ParserConfigurationException, SAXException, XPathExpressionException {
        IRI parent = RefNodeFactory.toIRI("http://example.org");
        List<Triple> nodes = new ArrayList<>();
        StatementEmitter emitter = nodes::add;
        InputStream is = getClass().getResourceAsStream("torch-portal-rss.xml");

        RegistryReaderRSS.parseRssFeed(parent, emitter, is);

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
    public void parseSymbiotaFeeds() throws XMLStreamException, IOException, ParserConfigurationException, SAXException, XPathExpressionException {
        IRI parent = RefNodeFactory.toIRI("http://example.org");
        List<Triple> nodes = new ArrayList<>();
        StatementEmitter emitter = nodes::add;
        InputStream is = getClass().getResourceAsStream("symbiota-rss.xml");

        RegistryReaderRSS.parseRssFeed(parent, emitter, is);

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
    public void parseIntermountainFeeds() throws XMLStreamException, IOException, ParserConfigurationException, SAXException, XPathExpressionException {
        IRI parent = RefNodeFactory.toIRI("http://example.org");
        List<Triple> nodes = new ArrayList<>();
        StatementEmitter emitter = nodes::add;
        InputStream is = getClass().getResourceAsStream("intermountain-biota-rss.xml");

        RegistryReaderRSS.parseRssFeed(parent, emitter, is);

        assertThat(nodes.size(), is(84));
    }

    @Test
    public void parseIPTRSS() throws XMLStreamException, IOException, ParserConfigurationException, SAXException, XPathExpressionException {
        IRI parent = RefNodeFactory.toIRI("http://example.org");
        List<Triple> nodes = new ArrayList<>();
        StatementEmitter emitter = nodes::add;
        InputStream is = getClass().getResourceAsStream("ipt-norway-rss.xml");
        assertNotNull(is);

        RegistryReaderRSS.parseRssFeed(parent, emitter, is);

        assertThat(nodes.size(), is(486));
    }


}