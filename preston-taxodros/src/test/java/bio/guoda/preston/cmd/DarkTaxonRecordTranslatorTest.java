package bio.guoda.preston.cmd;

import bio.guoda.preston.store.TestUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.IOUtils;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static junit.framework.TestCase.assertNotNull;
import static org.hamcrest.MatcherAssert.assertThat;

public class DarkTaxonRecordTranslatorTest {

    @Test
    public void photoDeposit() throws IOException {
        JsonNode multimedia = new ObjectMapper().readTree(getClass().getResourceAsStream("darktaxon/multimedia.json"));
        assertNotNull(multimedia);

        ObjectNode zenodoDeposit = DarkTaxonUtil.toPhotoDeposit(multimedia, getPublicationDateFactory(), Arrays.asList("mfn-test"));

        String actual = zenodoDeposit.toPrettyString();

        assertThat(actual, Is.is(IOUtils.toString(getClass().getResourceAsStream("darktaxon/multimedia-zenodo.json"), StandardCharsets.UTF_8)));

    }

    @Test
    public void eventDeposit() throws IOException {
        InputStream resourceAsStream = getClass().getResourceAsStream("darktaxon/event.json");
        assertNotNull(resourceAsStream);
        String jsonString = TestUtil.removeCarriageReturn(IOUtils.toString(resourceAsStream, StandardCharsets.UTF_8));

        ObjectNode zenodoDeposit = DarkTaxonUtil.toEventDeposit(jsonString, new PublicationDateFactory() {
            @Override
            public String getPublicationDate() {
                return "1999-12-31";
            }
        }, Arrays.asList("mfn-test"));
        String actual = zenodoDeposit.toPrettyString();

        assertThat(actual, Is.is(IOUtils.toString(getClass().getResourceAsStream("darktaxon/event-zenodo.json"), StandardCharsets.UTF_8)));


    }

    @Test
    public void physicalObjectDeposit() throws IOException {
        InputStream resourceAsStream = getClass().getResourceAsStream("darktaxon/occurrence.json");
        assertNotNull(resourceAsStream);
        String jsonString = TestUtil.removeCarriageReturn(IOUtils.toString(resourceAsStream, StandardCharsets.UTF_8));
        ObjectNode zenodoDeposit = DarkTaxonUtil.toPhysicalObjectDeposit(jsonString, getPublicationDateFactory(), Arrays.asList("mfn-test"));
        String actual = zenodoDeposit.toPrettyString();

        assertThat(actual, Is.is(IOUtils.toString(getClass().getResourceAsStream("darktaxon/occurrence-zenodo.json"), StandardCharsets.UTF_8)));
    }

    private PublicationDateFactory getPublicationDateFactory() {
        return new PublicationDateFactory() {
            @Override
            public String getPublicationDate() {
                return "1999-12-31";
            }
        };
    }

}
