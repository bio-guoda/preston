package org.globalbioticinteractions.preston.cmd;

import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Triple;
import org.globalbioticinteractions.preston.RefNodeConstants;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.util.List;

import static org.globalbioticinteractions.preston.model.RefNodeFactory.toBlank;
import static org.globalbioticinteractions.preston.model.RefNodeFactory.toIRI;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

public class CmdCrawlTest {

    @Test
    public void findCrawlInfo() {

        IRI someCrawlActivity = toIRI("http://example.org/crawl");

        List<Triple> crawlInfo = CmdCrawl.findCrawlInfo(someCrawlActivity);

        assertFalse(crawlInfo.isEmpty());

        crawlInfo.forEach(System.out::println);
    }

    @Test
    public void staticUUIDs() {
        String uuid = RefNodeConstants.ARCHIVE_COLLECTION.toString();

        assertThat(uuid, Is.is("0659a54f-b713-4f86-a917-5be166a14110"));


    }

}