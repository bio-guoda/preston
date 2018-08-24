package org.globalbioticinteractions.preston;

import java.io.IOException;

public interface Crawler {
    void crawl(DatasetListener listener) throws IOException;
}
