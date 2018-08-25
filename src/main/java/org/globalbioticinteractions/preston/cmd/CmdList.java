package org.globalbioticinteractions.preston.cmd;

import com.beust.jcommander.Parameters;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.globalbioticinteractions.preston.CrawlerGBIF;
import org.globalbioticinteractions.preston.Dataset;
import org.globalbioticinteractions.preston.DatasetListener;
import org.globalbioticinteractions.preston.DatasetType;

import java.io.IOException;

@Parameters(separators = "= ", commandDescription = "Show Version")
public class CmdList implements Runnable {

    private static final DatasetListener listener = new DatasetListenerCaching(new GBIFDatasetHandler(), new DatasetListenerLogging());

    @Override
    public void run() {
        try {
            new CrawlerGBIF().crawl(listener);
        } catch (IOException e) {
            throw new RuntimeException("failed to crawl GBIF datasets", e);
        }
    }

    private static class GBIFDatasetHandler implements DatasetListener {
        private final static Log LOG = LogFactory.getLog(GBIFDatasetHandler.class);

        @Override
        public void onDataset(Dataset dataset) {
            if (dataset.getType() == DatasetType.GBIF_DATASETS_JSON) {
                try {
                    CrawlerGBIF.parse(dataset.getData(), listener, dataset);
                } catch (IOException e) {
                    LOG.warn("failed to handle [" + dataset.getLabel() + "]", e);
                }
            }

        }
    }
}
