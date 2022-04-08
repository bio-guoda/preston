package bio.guoda.preston.cmd;

import bio.guoda.preston.RefNodeConstants;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.util.List;

import static bio.guoda.preston.RefNodeFactory.toIRI;
import static org.junit.Assert.assertFalse;
import static org.hamcrest.MatcherAssert.assertThat;

public class CmdActivityTest {

    @Test
    public void findCrawlInfo() {

        IRI someCrawlActivity = toIRI("http://example.org/crawl");

        List<Quad> crawlInfo = CmdActivity.findActivityInfo(new ActivityContext() {
            @Override
            public IRI getActivity() {
                return someCrawlActivity;
            }

            @Override
            public String getDescription() {
                return "this is an example crawl";
            }
        });

        assertFalse(crawlInfo.isEmpty());
    }

    @Test
    public void staticUUIDs() {
        String uuid = RefNodeConstants.BIODIVERSITY_DATASET_GRAPH_UUID.toString();

        assertThat(uuid, Is.is("0659a54f-b713-4f86-a917-5be166a14110"));


    }

}