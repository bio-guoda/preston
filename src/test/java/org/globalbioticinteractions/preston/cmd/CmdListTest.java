package org.globalbioticinteractions.preston.cmd;

import org.globalbioticinteractions.preston.process.GBIFRegistry;
import org.globalbioticinteractions.preston.process.Logging;
import org.globalbioticinteractions.preston.model.RefNode;
import org.globalbioticinteractions.preston.model.RefNodeCached;
import org.globalbioticinteractions.preston.model.RefNodeString;
import org.globalbioticinteractions.preston.model.RefNodeType;
import org.globalbioticinteractions.preston.model.RefNodeURI;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.StringStartsWith.startsWith;

public class CmdListTest {

    @Test
    public void parseDatasets() throws IOException {
        InputStream resourceAsStream = getClass().getResourceAsStream("gbifdatasets.json");

        final List<RefNode> refNodes = new ArrayList<>();

        GBIFRegistry.parse(resourceAsStream, refNodes::add, new RefNodeString(null, RefNodeType.UUID, "description"));

        assertThat(refNodes.size(), is(12));
        RefNode refNode = refNodes.get(0);
        assertThat(refNode.getType(), is(RefNodeType.UUID));
        assertThat(refNode.getLabel(), is("6555005d-4594-4a3e-be33-c70e587b63d7"));

        RefNode lastRefNode = refNodes.get(3);
        assertThat(lastRefNode.getType(), is(RefNodeType.URI));
        assertThat(lastRefNode.getLabel(), is("http://www.snib.mx/iptconabio/eml.do?r=SNIB-ME006-ME0061704F-ictioplancton-CH-SIB.2017.06.06"));

    }

    @Test
    public void printDataset() {
        String uuid = "38011dd0-386f-4f29-b6f2-5aecedac3190";
        String parentUUID = "23011dd0-386f-4f29-b6f2-5aecedac3190";
        RefNode parent = new RefNodeCached(new RefNodeString(null, RefNodeType.UUID, parentUUID), "parent-id");

        String str = Logging.printDataset(new RefNodeCached(new RefNodeString(parent, RefNodeType.URI, "http://example.com"), "some-id"));
        assertThat(str, startsWith("parent-id\tsome-id\thttp://example.com\tURI\t"));

        str = Logging.printDataset(new RefNodeCached(new RefNodeURI(parent, RefNodeType.DWCA, URI.create("https://example.com/some/data.zip")), "some-other-id"));
        assertThat(str, startsWith("parent-id\tsome-other-id\tdata@https://example.com/some/data.zip\tDWCA\t"));

        str = Logging.printDataset(new RefNodeString(parent, RefNodeType.UUID, uuid));
        assertThat(str, startsWith("parent-id\t\t38011dd0-386f-4f29-b6f2-5aecedac3190\tUUID\t"));
    }


}