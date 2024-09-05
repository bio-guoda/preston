package bio.guoda.preston.cmd;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Test;

import java.io.IOException;

import static junit.framework.TestCase.assertNotNull;

public class DarkTaxonRecordTranslatorTest {

    @Test
    public void photoDeposit() throws IOException {
        JsonNode multimedia = new ObjectMapper().readTree(getClass().getResourceAsStream("darktaxon/multimedia.json"));

        ObjectNode objectNode = new ObjectMapper().createObjectNode();



        assertNotNull(multimedia);
    }

    @Test
    public void eventDeposit() throws IOException {
        JsonNode event = new ObjectMapper().readTree(getClass().getResourceAsStream("darktaxon/event.json"));
        assertNotNull(event);
    }

    @Test
    public void physicalObjectDeposit() throws IOException {
        JsonNode physicalObject = new ObjectMapper().readTree(getClass().getResourceAsStream("darktaxon/occurrence.json"));
        assertNotNull(physicalObject);
    }

}
