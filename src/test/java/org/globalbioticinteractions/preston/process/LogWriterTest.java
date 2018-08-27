package org.globalbioticinteractions.preston.process;

import org.globalbioticinteractions.preston.model.RefNode;
import org.globalbioticinteractions.preston.model.RefNodeProxyData;
import org.globalbioticinteractions.preston.model.RefNodeString;
import org.globalbioticinteractions.preston.model.RefNodeType;
import org.globalbioticinteractions.preston.model.RefNodeURI;
import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringStartsWith.startsWith;

public class LogWriterTest {

    @Test
    public void printDataset() throws URISyntaxException {
        String uuid = "38011dd0-386f-4f29-b6f2-5aecedac3190";
        String parentUUID = "23011dd0-386f-4f29-b6f2-5aecedac3190";
        RefNode parent = new RefNodeProxyData(new RefNodeString(null, RefNodeType.UUID, parentUUID), "parent-id");

        File testFile = new File(getClass().getResource("test.txt").toURI());
        String str = LogWriter.printDataset(new RefNodeProxyData(new RefNodeString(parent, RefNodeType.URI, "http://example.com"), "some-id", testFile));
        assertThat(str, startsWith("parent-id\tsome-id\thttp://example.com\tURI\t19\t"));

        str = LogWriter.printDataset(new RefNodeProxyData(new RefNodeURI(parent, RefNodeType.DWCA, URI.create("https://example.com/some/data.zip")), "some-other-id", testFile));
        assertThat(str, startsWith("parent-id\tsome-other-id\tdata@https://example.com/some/data.zip\tDWCA\t19\t"));

        str = LogWriter.printDataset(new RefNodeString(parent, RefNodeType.UUID, uuid));
        assertThat(str, startsWith("parent-id\t\t38011dd0-386f-4f29-b6f2-5aecedac3190\tUUID\t36\t"));
    }


}