package org.globalbioticinteractions.preston.cmd;

import org.globalbioticinteractions.preston.CrawlerGBIF;
import org.globalbioticinteractions.preston.Dataset;
import org.globalbioticinteractions.preston.DatasetListener;
import org.globalbioticinteractions.preston.DatasetString;
import org.globalbioticinteractions.preston.DatasetType;
import org.globalbioticinteractions.preston.DatasetURI;
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

        final List<Dataset> datasets = new ArrayList<Dataset>();
        DatasetListener listener = datasets::add;

        CrawlerGBIF.parse(resourceAsStream, listener, new DatasetString(null, DatasetType.UUID, "description"));

        assertThat(datasets.size(), is(10));
        Dataset dataset = datasets.get(0);
        assertThat(dataset.getType(), is(DatasetType.UUID));
        assertThat(dataset.getLabel(), is("6555005d-4594-4a3e-be33-c70e587b63d7"));

        Dataset lastDataset = datasets.get(3);
        assertThat(lastDataset.getType(), is(DatasetType.URI));
        assertThat(lastDataset.getLabel(), is("http://www.snib.mx/iptconabio/eml.do?r=SNIB-ME006-ME0061704F-ictioplancton-CH-SIB.2017.06.06"));

    }

    @Test
    public void printDataset() {
        String uuid = "38011dd0-386f-4f29-b6f2-5aecedac3190";
        String parentUUID = "23011dd0-386f-4f29-b6f2-5aecedac3190";
        Dataset parent = new DatasetCached(new DatasetString(null, DatasetType.UUID, parentUUID), "parent-id");

        String str = DatasetListenerLogging.printDataset(new DatasetCached(new DatasetString(parent, DatasetType.URI, "http://example.com"), "some-id"));
        assertThat(str, startsWith("parent-id\tsome-id\thttp://example.com\tURI\t"));

        str = DatasetListenerLogging.printDataset(new DatasetCached(new DatasetURI(parent, DatasetType.DWCA, URI.create("https://example.com/some/data.zip")), "some-other-id"));
        assertThat(str, startsWith("parent-id\tsome-other-id\tdata@https://example.com/some/data.zip\tDWCA\t"));

        str = DatasetListenerLogging.printDataset(new DatasetString(parent, DatasetType.UUID, uuid));
        assertThat(str, startsWith("parent-id\t\t38011dd0-386f-4f29-b6f2-5aecedac3190\tUUID\t"));
    }


}