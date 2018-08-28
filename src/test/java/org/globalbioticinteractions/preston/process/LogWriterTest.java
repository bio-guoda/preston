package org.globalbioticinteractions.preston.process;

import org.globalbioticinteractions.preston.model.RefNodeProxyData;
import org.globalbioticinteractions.preston.model.RefNodeRelation;
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

        File testFile = new File(getClass().getResource("test.txt").toURI());
        String str = LogWriter.printDataset(new RefNodeProxyData(new RefNodeString(RefNodeType.URI, "http://example.com"), "some-id", testFile));
        assertThat(str, startsWith("some-id\thttp://example.com\tURI\t19\t"));

        str = LogWriter.printDataset(new RefNodeProxyData(new RefNodeURI(RefNodeType.DWCA, URI.create("https://example.com/some/data.zip")), "some-other-id", testFile));
        assertThat(str, startsWith("some-other-id\tdata@https://example.com/some/data.zip\tDWCA\t19\t"));

        str = LogWriter.printDataset(new RefNodeString(RefNodeType.UUID, uuid));
        assertThat(str, startsWith("61367a5b11a9f9a215b75e3455ac0f99b6a856aea06faa0667befca60edd0b06\t38011dd0-386f-4f29-b6f2-5aecedac3190\tUUID\t36\t"));

    }

    @Test
    public void relation() {
        RefNodeString source = new RefNodeString(RefNodeType.UUID, "source");
        RefNodeString relation = new RefNodeString(RefNodeType.UUID, "relation");
        RefNodeString target = new RefNodeString(RefNodeType.UUID, "target");

        String str = LogWriter.printDataset(new RefNodeRelation(source, relation, target));

        assertThat(str, startsWith("ce0d8e5c8ac18cb76b418972c7882f0b94e0bc2c952130e0511a1aa28c2ac9e0"));
        assertThat(str, startsWith("ce0d8e5c8ac18cb76b418972c7882f0b94e0bc2c952130e0511a1aa28c2ac9e0\t[source]<-[:relation]-[target]\tRELATION\t197\t"));

    }


}