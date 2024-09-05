package bio.guoda.preston.zenodo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class ZenodoMetadataFileStreamHandlerTest {

    @Test
    public void allowedDefinedPubDate() throws IOException {
        JsonNode jsonNode = new ObjectMapper().readTree(getClass().getResourceAsStream("batlit-data/example-batlit.json"));

        ZenodoContext contextDefault = new ZenodoContext("bla");
        assertTrue(ZenodoMetadataFileStreamHandler.hasAllowedPublicationDate(jsonNode, contextDefault));
    }

    @Test
    public void allowedEmptyPubDate() throws IOException {
        JsonNode jsonNode = new ObjectMapper().readTree(getClass().getResourceAsStream("batlit-data/example-batlit-no-pubdate.json"));

        ZenodoContext contextDefault = new ZenodoContext("bla");
        contextDefault.setAllowEmptyPublicationDate(true);
        assertTrue(ZenodoMetadataFileStreamHandler.hasAllowedPublicationDate(jsonNode, contextDefault));
    }

    @Test
    public void nonEmptyPubDateAllowed() throws IOException {
        JsonNode jsonNode = new ObjectMapper().readTree(getClass().getResourceAsStream("batlit-data/example-batlit.json"));

        ZenodoContext contextDefault = new ZenodoContext("bla");
        contextDefault.setAllowEmptyPublicationDate(false);
        assertTrue(ZenodoMetadataFileStreamHandler.hasAllowedPublicationDate(jsonNode, contextDefault));
    }

    @Test
    public void emptyPubDateNotAllowed() throws IOException {
        JsonNode jsonNode = new ObjectMapper().readTree(getClass().getResourceAsStream("batlit-data/example-batlit-no-pubdate.json"));

        ZenodoContext contextDefault = new ZenodoContext("bla");
        assertFalse(ZenodoMetadataFileStreamHandler.hasAllowedPublicationDate(jsonNode, contextDefault));
    }

    @Test
    public void emptyPubDateNotAllowed2() throws IOException {
        JsonNode jsonNode = new ObjectMapper().readTree(getClass().getResourceAsStream("batlit-data/example-batlit-no-pubdate.json"));

        ZenodoContext contextDefault = new ZenodoContext("bla");
        contextDefault.setAllowEmptyPublicationDate(false);
        assertFalse(ZenodoMetadataFileStreamHandler.hasAllowedPublicationDate(jsonNode, contextDefault));
    }

}