package org.globalbioticinteractions.preston;

import org.apache.commons.io.IOUtils;
import org.globalbioticinteractions.preston.cmd.DatasetListenerCaching;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;

public class CrawlerGBIFTest {

    @Test
    public void crawlSinglePage() throws IOException {
        AtomicInteger count = new AtomicInteger();
        DatasetListener listener = dataset -> count.incrementAndGet();
        new CrawlerGBIF().crawl(listener);
        assertThat(count.get(), is(not(0)));
    }



}