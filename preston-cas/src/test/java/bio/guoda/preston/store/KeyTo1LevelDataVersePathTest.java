package bio.guoda.preston.store;

import org.junit.Test;

import java.io.IOException;
import java.net.URI;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNotNull;

public class KeyTo1LevelDataVersePathTest {

    @Test
    public void parseFirstHit() throws IOException {
        URI firstHit = KeyTo1LevelDataVersePath.findFirstHit(getClass().getResourceAsStream("dataverse-search-result.json"));
        assertNotNull(firstHit);
        assertThat(firstHit, is(URI.create("https://dataverse.harvard.edu/api/access/datafile/2829688")));
    }

}