package org.globalbioticinteractions.preston;

import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.*;

public class CrawlerGBIFTest {

    @Test
    public void crawlSinglePage() throws IOException {
        AtomicInteger count = new AtomicInteger();
        DatasetListener listener = dataset -> count.incrementAndGet();
        new CrawlerGBIF().crawlPage(listener, 0, 2);
        assertThat(count.get(), is(not(0)));
    }

}