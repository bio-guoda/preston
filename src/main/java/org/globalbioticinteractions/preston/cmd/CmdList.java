package org.globalbioticinteractions.preston.cmd;

import com.beust.jcommander.Parameters;
import org.globalbioticinteractions.preston.CrawlerGBIF;
import org.globalbioticinteractions.preston.Dataset;
import org.globalbioticinteractions.preston.DatasetListener;
import org.globalbioticinteractions.preston.Preston;

import java.io.IOException;

@Parameters(separators = "= ", commandDescription = "Show Version")
public class CmdList implements Runnable {

    @Override
    public void run() {
        try {
            new CrawlerGBIF().crawl(new DatasetListener() {
                @Override
                public void onDataset(Dataset dataset) {
                    System.out.println(dataset.getUuid().toString() + "\t" + dataset.getUrl() + "\t" + dataset.getType().name());
                }
            });
        } catch (IOException e) {
            throw new RuntimeException("failed to craw GBIF datasets", e);
        }
        System.out.println(Preston.getVersion());
    }

}
