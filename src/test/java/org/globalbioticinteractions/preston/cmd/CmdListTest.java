package org.globalbioticinteractions.preston.cmd;

import org.apache.commons.lang3.StringUtils;
import org.globalbioticinteractions.preston.CrawlerGBIF;
import org.globalbioticinteractions.preston.Dataset;
import org.globalbioticinteractions.preston.DatasetListener;
import org.globalbioticinteractions.preston.DatasetType;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class CmdListTest {

    @Test
    public void parseDatasets() throws IOException {
        InputStream resourceAsStream = getClass().getResourceAsStream("gbifdatasets.json");

        final List<Dataset> datasets = new ArrayList<Dataset>();
        DatasetListener listener = new DatasetListener() {
            @Override
            public void onDataset(Dataset dataset) {
                datasets.add(dataset);
            }
        };

        assertThat(CrawlerGBIF.parse(resourceAsStream, listener), is(false));

        assertThat(datasets.size(), is(4));
        Dataset dataset = datasets.get(0);
        assertThat(dataset.getUuid(), is(UUID.fromString("6555005d-4594-4a3e-be33-c70e587b63d7")));
        assertThat(dataset.getType(), is(DatasetType.DARWIN_CORE_ARCHIVE));
        assertThat(dataset.getUrl(), is(URI.create("http://www.snib.mx/iptconabio/archive.do?r=SNIB-ME006-ME0061704F-ictioplancton-CH-SIB.2017.06.06")));

        Dataset lastDataset = datasets.get(3);
        assertThat(lastDataset.getUuid(), is(UUID.fromString("d0df772d-78f4-4602-acf2-7d768798f632")));
        assertThat(lastDataset.getType(), is(DatasetType.EML));
        assertThat(lastDataset.getUrl(), is(URI.create("http://www.snib.mx/iptconabio/eml.do?r=SNIB-ME006-ME0061612F-nematodos-CH")));

    }

    @Test
    public void generatePathFromUUID() {
        UUID uuid = UUID.fromString("d0df772d-78f4-4602-acf2-7d768798f632");
        assertThat(toPath(uuid), is("d0/df/77/d0df772d-78f4-4602-acf2-7d768798f632"));
    }

    private static String toPath(UUID uuid) {
        String uuidString = uuid.toString();
        String u0 = uuidString.substring(0, 2);
        String u1 = uuidString.substring(2, 4);
        String u2 = uuidString.substring(4, 6);
        return StringUtils.join(Arrays.asList(u0, u1, u2, uuidString), "/");
    }

}