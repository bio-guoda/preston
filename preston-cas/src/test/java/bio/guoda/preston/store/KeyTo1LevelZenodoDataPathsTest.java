package bio.guoda.preston.store;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

import static org.junit.Assert.*;

public class KeyTo1LevelZenodoDataPathsTest {

    @Test
    public void findDataZipContentId() throws IOException {

        JsonNode jsonNode =
                new ObjectMapper()
                        .readTree(getClass().getResourceAsStream("data-archive-record-result.json"));
        URI uri = KeyTo1LevelZenodoDataPaths.parseResult(   "bla", jsonNode);
        assertThat(uri.toString(), Is.is("hash://md5/b871e22f0e8c576305f99cb5aff8cddd"));
    }

    @Test
    public void findDataZipContentId2023() throws IOException {

        JsonNode jsonNode =
                new ObjectMapper()
                        .readTree(getClass().getResourceAsStream("data-archive-record-result-2023.json"));
        URI uri = KeyTo1LevelZenodoDataPaths.parseResult("bla", jsonNode);
        assertThat(uri.toString(), Is.is("hash://md5/b871e22f0e8c576305f99cb5aff8cddd"));
    }

}