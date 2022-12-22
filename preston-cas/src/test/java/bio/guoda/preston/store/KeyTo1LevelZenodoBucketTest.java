package bio.guoda.preston.store;

import org.junit.Test;

import java.net.URI;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class KeyTo1LevelZenodoBucketTest {

    @Test
    public void parseBucketEndpoint() {
        URI fileURI = URI.create("https://zenodo.org/api/files/a2a67842-dfa0-42db-aa22-571d8b79c902/9d1130a7337da445c9211f078502cdcaee116a5b9ac9a12e14bfa72ccb0a6828");
        String endpoint = KeyTo1LevelZenodoBucket.parseZenodoBucketEndpoint(fileURI);

        assertThat(endpoint,
                is("https://zenodo.org/api/files/a2a67842-dfa0-42db-aa22-571d8b79c902/"));
    }

}