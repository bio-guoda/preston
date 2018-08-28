package org.globalbioticinteractions.preston.process;

import com.sun.syndication.io.FeedException;
import org.globalbioticinteractions.preston.model.RefNode;
import org.globalbioticinteractions.preston.model.RefNodeString;
import org.globalbioticinteractions.preston.model.RefNodeType;
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

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class RegistryReaderIDigBioTest {

    @Test
    public void parsePublishers() throws IOException {

        RefNode providedParent = new RefNodeString(RefNodeType.UUID, "someRegistryUUID");
        final List<RefNode> nodes = new ArrayList<>();

        InputStream is = getClass().getResourceAsStream("idigbio-publishers.json");

        RegistryReaderIDigBio.parsePublishers(providedParent, nodes::add, is);

        assertThat(nodes.size(), is(468));

        RefNode node = nodes.get(0);
        assertThat(node.getLabel(), is("51290816-f682-4e38-a06c-03bf5df2442d"));
        assertThat(node.getType(), is(RefNodeType.UUID));

        node = nodes.get(1);
        assertThat(node.getLabel(), is("[someRegistryUUID]<-[:http://example.org/partOf]-[51290816-f682-4e38-a06c-03bf5df2442d]"));
        assertThat(node.getType(), is(RefNodeType.RELATION));

        node = nodes.get(2);
        assertThat(node.getLabel(), is("https://www.morphosource.org/rss/ms.rss"));
        assertThat(node.getType(), is(RefNodeType.URI));

        node = nodes.get(3);
        assertThat(node.getLabel(), is("[51290816-f682-4e38-a06c-03bf5df2442d]<-[:http://example.org/feedOf]-[https://www.morphosource.org/rss/ms.rss]"));
        assertThat(node.getType(), is(RefNodeType.RELATION));

        node = nodes.get(4);
        assertThat(node.getLabel(), is("data@https://www.morphosource.org/rss/ms.rss"));
        assertThat(node.getType(), is(RefNodeType.IDIGBIO_RSS));

        node = nodes.get(5);
        assertThat(node.getLabel(), is("[https://www.morphosource.org/rss/ms.rss]<-[:http://example.org/dereferenceOf]-[data@https://www.morphosource.org/rss/ms.rss]"));
        assertThat(node.getType(), is(RefNodeType.RELATION));

        node = nodes.get(6);
        assertThat(node.getLabel(), is("a9684883-ce9b-4be1-9841-b063fc69e163"));
        assertThat(node.getType(), is(RefNodeType.UUID));

    }

    @Test
    public void parseFeeds() throws XMLStreamException, IOException, FeedException, ParserConfigurationException, SAXException, XPathExpressionException {
        RefNode parent = new RefNodeString(RefNodeType.URI, "http://example.org");
        List<RefNode> nodes = new ArrayList<>();
        RefNodeEmitter emitter = nodes::add;
        InputStream is = getClass().getResourceAsStream("torch-portal-rss.xml");

        RegistryReaderIDigBio.parseRssFeed(parent, emitter, is);

        assertThat(nodes.size(), is(28));

        List<String> labels = nodes.stream().limit(7).map(RefNode::getLabel).collect(Collectors.toList());

        assertThat(labels, is(Arrays.asList("fea81a47-2365-45cc-bef9-b6bbff7457e6",
                "http://portal.torcherbaria.org/portal/content/dwca/BRIT_DwC-A.eml",
                "data@http://portal.torcherbaria.org/portal/content/dwca/BRIT_DwC-A.eml",
                "[http://portal.torcherbaria.org/portal/content/dwca/BRIT_DwC-A.eml]<-[:http://example.org/dereferenceOf]-[data@http://portal.torcherbaria.org/portal/content/dwca/BRIT_DwC-A.eml]",
                "http://portal.torcherbaria.org/portal/content/dwca/BRIT_DwC-A.zip",
                "data@http://portal.torcherbaria.org/portal/content/dwca/BRIT_DwC-A.zip",
                "[http://portal.torcherbaria.org/portal/content/dwca/BRIT_DwC-A.zip]<-[:http://example.org/dereferenceOf]-[data@http://portal.torcherbaria.org/portal/content/dwca/BRIT_DwC-A.zip]"
        )));

    }

    @Test
    public void parseSymbiotaFeeds() throws XMLStreamException, IOException, FeedException, ParserConfigurationException, SAXException, XPathExpressionException {
        RefNode parent = new RefNodeString(RefNodeType.URI, "http://example.org");
        List<RefNode> nodes = new ArrayList<>();
        RefNodeEmitter emitter = nodes::add;
        InputStream is = getClass().getResourceAsStream("symbiota-rss.xml");

        RegistryReaderIDigBio.parseRssFeed(parent, emitter, is);

        assertThat(nodes.size(), is(189));

        List<String> labels = nodes.stream().limit(7).map(RefNode::getLabel).collect(Collectors.toList());

        assertThat(labels, is(Arrays.asList("4b9c73cc-d12d-4654-bdfb-081dce21729b",
                "http://midwestherbaria.org/portal/content/dwca/ALBC_DwC-A.eml",
                "data@http://midwestherbaria.org/portal/content/dwca/ALBC_DwC-A.eml",
                "[http://midwestherbaria.org/portal/content/dwca/ALBC_DwC-A.eml]<-[:http://example.org/dereferenceOf]-[data@http://midwestherbaria.org/portal/content/dwca/ALBC_DwC-A.eml]",
                "http://midwestherbaria.org/portal/content/dwca/ALBC_DwC-A.zip",
                "data@http://midwestherbaria.org/portal/content/dwca/ALBC_DwC-A.zip",
                "[http://midwestherbaria.org/portal/content/dwca/ALBC_DwC-A.zip]<-[:http://example.org/dereferenceOf]-[data@http://midwestherbaria.org/portal/content/dwca/ALBC_DwC-A.zip]")));

        List<String> types = nodes.stream().limit(7).map(n -> n.getType().toString()).collect(Collectors.toList());

        assertThat(types, is(Arrays.asList("UUID", "URI", "EML", "RELATION", "URI", "DWCA", "RELATION")));

    }

    @Test
    public void parseIntermountainFeeds() throws XMLStreamException, IOException, FeedException, ParserConfigurationException, SAXException, XPathExpressionException {
        RefNode parent = new RefNodeString(RefNodeType.URI, "http://example.org");
        List<RefNode> nodes = new ArrayList<>();
        RefNodeEmitter emitter = nodes::add;
        InputStream is = getClass().getResourceAsStream("intermountain-biota-rss.xml");

        RegistryReaderIDigBio.parseRssFeed(parent, emitter, is);

        assertThat(nodes.size(), is(84));
    }


}