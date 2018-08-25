package org.globalbioticinteractions.preston;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
        new CrawlerGBIF().crawlPage(listener, new DatasetString(null, DatasetType.URI, "https://api.gbif.org/v1/dataset?offset=" + 0 + "&limit=" + 2));
        assertThat(count.get(), is(not(0)));
    }

    @Test
    public void testSHA256() throws IOException {
        assertSHA(CrawlerGBIF.calcSHA256(IOUtils.toInputStream("something", StandardCharsets.UTF_8), new ByteArrayOutputStream()));

        assertSHA(CrawlerGBIF.calcSHA256("something"));
    }

    private void assertSHA(String calculated) {
        assertThat(calculated, is("3fc9b689459d738f8c88a3a48aa9e33542016b7a4052e001aaa536fca74813cb"));
        assertThat(calculated.length(), is(64));
    }

    @Test
    public void generatePathFromUUID() {
        assertThat(CrawlerGBIF.toPath("3fc9b689459d738f8c88a3a48aa9e33542016b7a4052e001aaa536fca74813cb"),
                is("3f/c9/b6/3fc9b689459d738f8c88a3a48aa9e33542016b7a4052e001aaa536fca74813cb"));
    }


}