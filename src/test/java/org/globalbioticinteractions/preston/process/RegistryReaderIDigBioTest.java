package org.globalbioticinteractions.preston.process;

import com.sun.syndication.io.FeedException;
import org.apache.commons.lang3.StringUtils;
import org.globalbioticinteractions.preston.Seeds;
import org.globalbioticinteractions.preston.model.RefNode;
import org.globalbioticinteractions.preston.model.RefStatement;
import org.globalbioticinteractions.preston.model.RefNodeString;
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

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class RegistryReaderIDigBioTest {

    @Test
    public void onSeed() {
        ArrayList<RefStatement> nodes = new ArrayList<>();
        RegistryReaderIDigBio reader = new RegistryReaderIDigBio(nodes::add);
        RefNodeString bla = new RefNodeString("bla");
        reader.on(new RefStatement(bla, bla, Seeds.SEED_NODE_IDIGBIO));
        assertThat(nodes.size(), is(2));
    }

    @Test
    public void onRegistry() {
        ArrayList<RefStatement> nodes = new ArrayList<>();
        RegistryReaderIDigBio reader = new RegistryReaderIDigBio(nodes::add);
        RefNodeString bla = new RefNodeString("bla");
        reader.on(new RefStatement(bla, bla, Seeds.SEED_NODE_IDIGBIO));
        assertThat(nodes.size(), is(2));
    }

    @Test
    public void parsePublishers() throws IOException {

        RefNode providedParent = new RefNodeString("someRegistryUUID");
        final List<RefStatement> nodes = new ArrayList<>();

        InputStream is = getClass().getResourceAsStream("idigbio-publishers.json");

        RegistryReaderIDigBio.parsePublishers(providedParent, nodes::add, is);

        assertThat(nodes.size(), is(312));

        RefStatement node = nodes.get(0);
        assertThat(node.getLabel(), is("[someRegistryUUID]-[:http://www.w3.org/ns/prov#hadMember]->[51290816-f682-4e38-a06c-03bf5df2442d]"));

        node = nodes.get(1);
        assertThat(node.getLabel(), is("[51290816-f682-4e38-a06c-03bf5df2442d]-[:http://example.org/hasFeed]->[https://www.morphosource.org/rss/ms.rss]"));

        node = nodes.get(2);
        assertThat(node.getLabel(), is("[https://www.morphosource.org/rss/ms.rss]-[:http://purl.org/dc/elements/1.1/format]->[application/rss+xml]"));

        node = nodes.get(3);
        assertThat(node.getLabel(), is("[?]-[:http://www.w3.org/ns/prov#wasDerivedFrom]->[https://www.morphosource.org/rss/ms.rss]"));

        node = nodes.get(4);
        assertThat(node.getLabel(), is("[someRegistryUUID]-[:http://www.w3.org/ns/prov#hadMember]->[a9684883-ce9b-4be1-9841-b063fc69e163]"));

        node = nodes.get(5);
        assertThat(node.getLabel(), is("[a9684883-ce9b-4be1-9841-b063fc69e163]-[:http://example.org/hasFeed]->[http://portal.torcherbaria.org/portal/webservices/dwc/rss.xml]"));

        node = nodes.get(6);
        assertThat(node.getLabel(), is("[http://portal.torcherbaria.org/portal/webservices/dwc/rss.xml]-[:http://purl.org/dc/elements/1.1/format]->[application/rss+xml]"));

        node = nodes.get(7);
        assertThat(node.getLabel(), is("[?]-[:http://www.w3.org/ns/prov#wasDerivedFrom]->[http://portal.torcherbaria.org/portal/webservices/dwc/rss.xml]"));

        node = nodes.get(8);
        assertThat(node.getLabel(), is("[someRegistryUUID]-[:http://www.w3.org/ns/prov#hadMember]->[089a51fa-5f81-48e7-a1b7-9bc539555f29]"));

    }

    @Test
    public void parseFeeds() throws XMLStreamException, IOException, FeedException, ParserConfigurationException, SAXException, XPathExpressionException {
        RefNode parent = new RefNodeString("http://example.org");
        List<RefStatement> nodes = new ArrayList<>();
        RefStatementEmitter emitter = nodes::add;
        InputStream is = getClass().getResourceAsStream("torch-portal-rss.xml");

        RegistryReaderIDigBio.parseRssFeed(parent, emitter, is);

        assertThat(nodes.size(), is(28));

        List<String> actual = nodes.stream().limit(9).map(RefStatement::getLabel).collect(Collectors.toList());

        List<String> expected = Arrays.asList("[http://example.org]-[:http://www.w3.org/ns/prov#hadMember]->[fea81a47-2365-45cc-bef9-b6bbff7457e6]",
                "[fea81a47-2365-45cc-bef9-b6bbff7457e6]-[:http://www.w3.org/ns/prov#hadMember]->[http://portal.torcherbaria.org/portal/content/dwca/BRIT_DwC-A.eml]",
                "[http://portal.torcherbaria.org/portal/content/dwca/BRIT_DwC-A.eml]-[:http://purl.org/dc/elements/1.1/format]->[application/eml]",
                "[?]-[:http://www.w3.org/ns/prov#wasDerivedFrom]->[http://portal.torcherbaria.org/portal/content/dwca/BRIT_DwC-A.eml]",
                "[fea81a47-2365-45cc-bef9-b6bbff7457e6]-[:http://www.w3.org/ns/prov#hadMember]->[http://portal.torcherbaria.org/portal/content/dwca/BRIT_DwC-A.zip]",
                "[http://portal.torcherbaria.org/portal/content/dwca/BRIT_DwC-A.zip]-[:http://purl.org/dc/elements/1.1/format]->[application/dwca]",
                "[?]-[:http://www.w3.org/ns/prov#wasDerivedFrom]->[http://portal.torcherbaria.org/portal/content/dwca/BRIT_DwC-A.zip]",
                "[http://example.org]-[:http://www.w3.org/ns/prov#hadMember]->[bee3f1bd-8944-4593-8c37-d64a7c9bc1e1]",
                "[bee3f1bd-8944-4593-8c37-d64a7c9bc1e1]-[:http://www.w3.org/ns/prov#hadMember]->[http://portal.torcherbaria.org/portal/content/dwca/HPC_DwC-A.eml]");
        assertThat(actual.size(), is(expected.size()));

        for (int i=0 ; i< expected.size(); i++) {
            assertThat(expected.get(i), is(actual.get(i)));
        }
        assertThat(actual, is(expected));
    }

    @Test
    public void parseSymbiotaFeeds() throws XMLStreamException, IOException, FeedException, ParserConfigurationException, SAXException, XPathExpressionException {
        RefNode parent = new RefNodeString("http://example.org");
        List<RefStatement> nodes = new ArrayList<>();
        RefStatementEmitter emitter = nodes::add;
        InputStream is = getClass().getResourceAsStream("symbiota-rss.xml");

        RegistryReaderIDigBio.parseRssFeed(parent, emitter, is);

        assertThat(nodes.size(), is(189));

        List<String> labels = nodes.stream().limit(8).map(RefStatement::getLabel).collect(Collectors.toList());

        assertThat(labels, is(Arrays.asList(
                "[http://example.org]-[:http://www.w3.org/ns/prov#hadMember]->[4b9c73cc-d12d-4654-bdfb-081dce21729b]",
                "[4b9c73cc-d12d-4654-bdfb-081dce21729b]-[:http://www.w3.org/ns/prov#hadMember]->[http://midwestherbaria.org/portal/content/dwca/ALBC_DwC-A.eml]",
                "[http://midwestherbaria.org/portal/content/dwca/ALBC_DwC-A.eml]-[:http://purl.org/dc/elements/1.1/format]->[application/eml]",
                "[?]-[:http://www.w3.org/ns/prov#wasDerivedFrom]->[http://midwestherbaria.org/portal/content/dwca/ALBC_DwC-A.eml]",
                "[4b9c73cc-d12d-4654-bdfb-081dce21729b]-[:http://www.w3.org/ns/prov#hadMember]->[http://midwestherbaria.org/portal/content/dwca/ALBC_DwC-A.zip]",
                "[http://midwestherbaria.org/portal/content/dwca/ALBC_DwC-A.zip]-[:http://purl.org/dc/elements/1.1/format]->[application/dwca]",
                "[?]-[:http://www.w3.org/ns/prov#wasDerivedFrom]->[http://midwestherbaria.org/portal/content/dwca/ALBC_DwC-A.zip]",
                "[http://example.org]-[:http://www.w3.org/ns/prov#hadMember]->[b01789b2-c5d7-11e4-b6af-00163e00498d]")));

    }

    @Test
    public void parseIntermountainFeeds() throws XMLStreamException, IOException, FeedException, ParserConfigurationException, SAXException, XPathExpressionException {
        RefNode parent = new RefNodeString("http://example.org");
        List<RefStatement> nodes = new ArrayList<>();
        RefStatementEmitter emitter = nodes::add;
        InputStream is = getClass().getResourceAsStream("intermountain-biota-rss.xml");

        RegistryReaderIDigBio.parseRssFeed(parent, emitter, is);

        assertThat(nodes.size(), is(84));
    }


}