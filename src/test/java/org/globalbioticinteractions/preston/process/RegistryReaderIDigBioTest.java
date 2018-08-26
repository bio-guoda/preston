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

        RefNode providedParent = new RefNodeString(null, RefNodeType.UUID, "bla");
        final List<RefNode> nodes = new ArrayList<>();

        InputStream is = getClass().getResourceAsStream("idigbio-publishers.json");

        RegistryReaderIDigBio.parsePublishers(providedParent, nodes::add, is);

        assertThat(nodes.size(), is(234));

        RefNode first = nodes.get(0);
        assertThat(first.getLabel(), is("51290816-f682-4e38-a06c-03bf5df2442d"));
        assertThat(first.getType(), is(RefNodeType.UUID));
        assertTrue(first.getParent().equivalentTo(providedParent));

        RefNode second = nodes.get(1);
        assertThat(second.getLabel(), is("https://www.morphosource.org/rss/ms.rss"));
        assertThat(second.getType(), is(RefNodeType.URI));
        assertTrue(second.getParent().equivalentTo(first));

        RefNode third = nodes.get(2);
        assertThat(third.getLabel(), is("data@https://www.morphosource.org/rss/ms.rss"));
        assertThat(third.getType(), is(RefNodeType.IDIGBIO_RSS));
        assertTrue(third.getParent().equivalentTo(second));

        RefNode fourth = nodes.get(3);
        assertThat(fourth.getLabel(), is("a9684883-ce9b-4be1-9841-b063fc69e163"));
        assertThat(fourth.getType(), is(RefNodeType.UUID));
        assertTrue(fourth.getParent().equivalentTo(providedParent));

    }

    @Test
    public void parseFeeds() throws XMLStreamException, IOException, FeedException, ParserConfigurationException, SAXException, XPathExpressionException {
        RefNode parent = new RefNodeString(null, RefNodeType.URI, "http://example.org");
        List<RefNode> nodes = new ArrayList<>();
        RefNodeEmitter emitter = nodes::add;
        InputStream is = getClass().getResourceAsStream("torch-portal-rss.xml");

        RegistryReaderIDigBio.parseRssFeed(parent, emitter, is);

        for (RefNode node : nodes) {
            System.out.println(node.getLabel());
        }

        assertThat(nodes.size(), is(20));

        List<String> labels = nodes.stream().limit(5).map(RefNode::getLabel).collect(Collectors.toList());

        assertThat(labels, is(Arrays.asList("fea81a47-2365-45cc-bef9-b6bbff7457e6",
                "http://portal.torcherbaria.org/portal/content/dwca/BRIT_DwC-A.eml",
                "data@http://portal.torcherbaria.org/portal/content/dwca/BRIT_DwC-A.eml",
                "http://portal.torcherbaria.org/portal/content/dwca/BRIT_DwC-A.zip",
                "data@http://portal.torcherbaria.org/portal/content/dwca/BRIT_DwC-A.zip")));

        List<String> parentLabels = nodes.stream().limit(5).map(node -> node.getParent() == null ? "" : node.getParent().getLabel()).collect(Collectors.toList());

        assertThat(parentLabels, is(Arrays.asList("http://example.org",
                "fea81a47-2365-45cc-bef9-b6bbff7457e6",
                "http://portal.torcherbaria.org/portal/content/dwca/BRIT_DwC-A.eml",
                "fea81a47-2365-45cc-bef9-b6bbff7457e6",
                "http://portal.torcherbaria.org/portal/content/dwca/BRIT_DwC-A.zip")));

    }


}