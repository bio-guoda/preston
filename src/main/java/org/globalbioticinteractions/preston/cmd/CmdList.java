package org.globalbioticinteractions.preston.cmd;

import com.beust.jcommander.Parameters;
import org.globalbioticinteractions.preston.CrawlerGBIF;
import org.globalbioticinteractions.preston.Dataset;
import org.globalbioticinteractions.preston.HashFactory;
import org.globalbioticinteractions.preston.HashFactoryNull;
import org.globalbioticinteractions.preston.Preston;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;
import java.util.Date;

@Parameters(separators = "= ", commandDescription = "Show Version")
public class CmdList implements Runnable {

    @Override
    public void run() {
        try {
            new CrawlerGBIF()
                    .crawl(dataset -> {
                        System.out.println(printDataset(dataset, new HashFactoryNull()));
                    });
        } catch (IOException e) {
            throw new RuntimeException("failed to craw GBIF datasets", e);
        }
        System.out.println(Preston.getVersion());
    }

    String printDataset(Dataset dataset, HashFactory hashFactory) {
        String parentUUID = (dataset.getParent() == null ? "" : hashFactory.hashFor(dataset.getParent()));
        String uuid = hashFactory.hashFor(dataset);
        String accessedAt = ISODateTimeFormat.dateTime().withZoneUTC().print(new Date().getTime());
        return (parentUUID + "\t" + uuid + "\t" + dataset.getLabel() + "\t" + dataset.getType().name() + "\t" + accessedAt);
    }

}
