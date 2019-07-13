package bio.guoda.preston.cmd;

import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Triple;
import bio.guoda.preston.RefNodeConstants;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.util.List;

import static bio.guoda.preston.model.RefNodeFactory.toBlank;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

public class CmdActivityTest {

    @Test
    public void findCrawlInfo() {

        IRI someCrawlActivity = toIRI("http://example.org/crawl");

        List<Triple> crawlInfo = CmdActivity.findCrawlInfo(someCrawlActivity);

        assertFalse(crawlInfo.isEmpty());

        crawlInfo.forEach(System.out::println);
    }

    @Test
    public void staticUUIDs() {
        String uuid = RefNodeConstants.ARCHIVE_COLLECTION.toString();

        assertThat(uuid, Is.is("0659a54f-b713-4f86-a917-5be166a14110"));


    }

}