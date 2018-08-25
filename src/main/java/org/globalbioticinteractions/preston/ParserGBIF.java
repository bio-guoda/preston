package org.globalbioticinteractions.preston;

import java.io.IOException;
import java.io.InputStream;

public class ParserGBIF {
    private final Dataset parentDataset;
    private final DatasetListener listener;
    private boolean endOfRecords = false;

    public ParserGBIF(DatasetListener listener, Dataset parentDataset) {
        this.listener = listener;
        this.parentDataset = parentDataset;
    }

    public void handle(InputStream content) throws IOException {
        endOfRecords = CrawlerGBIF.parse(content, this.listener, this.parentDataset);
    }

    public boolean reachedEndOfRecords() {
        return endOfRecords;
    }
}
