package org.globalbioticinteractions.preston.cmd;

import org.globalbioticinteractions.preston.CrawlerGBIF;
import org.globalbioticinteractions.preston.Dataset;
import org.globalbioticinteractions.preston.DatasetListener;
import org.globalbioticinteractions.preston.DatasetString;
import org.globalbioticinteractions.preston.DatasetType;
import org.globalbioticinteractions.preston.DatasetURI;
import org.globalbioticinteractions.preston.HashFactory;
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
        DatasetListener listener = dataset -> {
            System.out.println(new CmdList().printDataset(dataset, new HashFactory() {

                @Override
                public String hashFor(Dataset dataset) {
                    return dataset.getLabel();
                }
            }));
            datasets.add(dataset);
        };

        assertThat(CrawlerGBIF.parse(resourceAsStream, listener, new DatasetString(null, DatasetType.UUID, "description")), is(false));

        HashFactory hashFactory = new HashFactory() {

            @Override
            public String hashFor(Dataset dataset) {
                return dataset.getLabel();
            }
        };

        assertThat(datasets.size(), is(10));
        Dataset dataset = datasets.get(0);
        assertThat(hashFactory.hashFor(dataset), is("a5359fd66338245a438e50d7889d8a242d6967ef0639e29d1ff3e6fd6c6a9d45"));
        assertThat(dataset.getType(), is(DatasetType.UUID));
        assertThat(dataset.getLabel(), is("6555005d-4594-4a3e-be33-c70e587b63d7"));

        Dataset lastDataset = datasets.get(3);
        assertThat(hashFactory.hashFor(lastDataset), is("ded1c510d73b630c281753d6899f016bf1350308bcf522bd5661d19b696e58d8"));
        assertThat(lastDataset.getType(), is(DatasetType.URI));
        assertThat(lastDataset.getLabel(), is("http://www.snib.mx/iptconabio/eml.do?r=SNIB-ME006-ME0061704F-ictioplancton-CH-SIB.2017.06.06"));

    }

    @Test
    public void printDataset() {
        String uuid = "38011dd0-386f-4f29-b6f2-5aecedac3190";
        String parentUUID = "23011dd0-386f-4f29-b6f2-5aecedac3190";
        DatasetString parent = new DatasetString(null, DatasetType.UUID, parentUUID);
        HashFactory hashFactory = dataset -> "hashOf@" + dataset.getLabel();
        String str = new CmdList().printDataset(new DatasetString(parent, DatasetType.URI, "http://example.com"), hashFactory);
        assertThat(str, startsWith("hashOf@23011dd0-386f-4f29-b6f2-5aecedac3190\thashOf@http://example.com\thttp://example.com\tURI\t"));

        str = new CmdList().printDataset(new DatasetURI(parent, DatasetType.DWCA, URI.create("https://example.com/some/data.zip")), hashFactory);
        assertThat(str, startsWith("hashOf@23011dd0-386f-4f29-b6f2-5aecedac3190\thashOf@data@https://example.com/some/data.zip\tdata@https://example.com/some/data.zip\tDWCA\t"));

        str = new CmdList().printDataset(new DatasetString(parent, DatasetType.UUID, uuid), hashFactory);
        assertThat(str, startsWith("hashOf@23011dd0-386f-4f29-b6f2-5aecedac3190\thashOf@38011dd0-386f-4f29-b6f2-5aecedac3190\t38011dd0-386f-4f29-b6f2-5aecedac3190\tUUID\t"));
    }


}