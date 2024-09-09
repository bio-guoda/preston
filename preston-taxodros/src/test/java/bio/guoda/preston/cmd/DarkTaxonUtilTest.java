package bio.guoda.preston.cmd;

import bio.guoda.preston.store.TestUtil;
import bio.guoda.preston.zenodo.ZenodoContext;
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

public class DarkTaxonUtilTest {

    @Test
    public void photoDeposit() throws IOException, MissingMetadataFieldException {
        JsonNode multimedia = new ObjectMapper().readTree(getClass().getResourceAsStream("darktaxon/multimedia.json"));
        assertNotNull(multimedia);

        ObjectNode zenodoDeposit = DarkTaxonUtil.toPhotoDeposit(multimedia, getPublicationDateFactory(), getTestConfig());

        String actual = zenodoDeposit.toPrettyString();
        System.out.println(actual);

        assertThat(actual, Is.is(IOUtils.toString(getClass().getResourceAsStream("darktaxon/multimedia-zenodo.json"), StandardCharsets.UTF_8)));

    }

    @Test(expected = MissingMetadataFieldException.class)
    public void photoDepositMissingData() throws IOException, MissingMetadataFieldException {
        JsonNode multimedia = new ObjectMapper().readTree(getClass().getResourceAsStream("darktaxon/multimedia-missing-lsid.json"));
        assertNotNull(multimedia);

        DarkTaxonUtil.toPhotoDeposit(multimedia, getPublicationDateFactory(), getTestConfig());

    }

    @Test
    public void photoDepositMissingRecordedById() throws IOException, MissingMetadataFieldException {
        InputStream resourceAsStream = getClass().getResourceAsStream("darktaxon/event-missing-recorded-by-id.json");
        assertNotNull(resourceAsStream);
        String jsonString = TestUtil.removeCarriageReturn(IOUtils.toString(resourceAsStream, StandardCharsets.UTF_8));

        ObjectNode eventDeposit = DarkTaxonUtil.toEventDeposit(jsonString, getPublicationDateFactory(), getTestConfig());

        String actual = eventDeposit.toPrettyString();

        System.out.println(actual);

        assertThat(actual, Is.is(IOUtils.toString(getClass().getResourceAsStream("darktaxon/event-zenodo-missing-recorded-by-id.json"), StandardCharsets.UTF_8)));

    }

    @Test
    public void eventDeposit() throws IOException, MissingMetadataFieldException {
        InputStream resourceAsStream = getClass().getResourceAsStream("darktaxon/event.json");
        assertNotNull(resourceAsStream);
        String jsonString = TestUtil.removeCarriageReturn(IOUtils.toString(resourceAsStream, StandardCharsets.UTF_8));

        ObjectNode zenodoDeposit = DarkTaxonUtil.toEventDeposit(jsonString, new PublicationDateFactory() {
            @Override
            public String getPublicationDate() {
                return "1999-12-31";
            }
        }, getTestConfig());
        String actual = zenodoDeposit.toPrettyString();

        System.out.println(actual);

        assertThat(actual, Is.is(IOUtils.toString(getClass().getResourceAsStream("darktaxon/event-zenodo.json"), StandardCharsets.UTF_8)));


    }
    @Test(expected = MissingMetadataFieldException.class)
    public void eventDepositMissingData() throws IOException, MissingMetadataFieldException {
        InputStream resourceAsStream = getClass().getResourceAsStream("darktaxon/event-missing-lsid.json");
        assertNotNull(resourceAsStream);
        String jsonString = TestUtil.removeCarriageReturn(IOUtils.toString(resourceAsStream, StandardCharsets.UTF_8));

        DarkTaxonUtil.toEventDeposit(jsonString, new PublicationDateFactory() {
            @Override
            public String getPublicationDate() {
                return "1999-12-31";
            }
        }, getTestConfig());
    }

    private ZenodoContext getTestConfig() {
        return new ZenodoContext("SECRET", "https://sandbox.zenodo.org", Arrays.asList("mfn-test"));
    }

    @Test
    public void physicalObjectDeposit() throws IOException, MissingMetadataFieldException {
        InputStream resourceAsStream = getClass().getResourceAsStream("darktaxon/occurrence.json");
        assertNotNull(resourceAsStream);
        String jsonString = TestUtil.removeCarriageReturn(IOUtils.toString(resourceAsStream, StandardCharsets.UTF_8));
        ObjectNode zenodoDeposit = DarkTaxonUtil.toPhysicalObjectDeposit(jsonString, getPublicationDateFactory(), getTestConfig());
        String actual = zenodoDeposit.toPrettyString();
        System.out.println(actual);

        assertThat(actual, Is.is(IOUtils.toString(getClass().getResourceAsStream("darktaxon/occurrence-zenodo.json"), StandardCharsets.UTF_8)));
    }


    @Test(expected = MissingMetadataFieldException.class)
    public void physicalObjectDepositMissingData() throws IOException, MissingMetadataFieldException {
        InputStream resourceAsStream = getClass().getResourceAsStream("darktaxon/occurrence-missing-lsid.json");
        assertNotNull(resourceAsStream);
        String jsonString = TestUtil.removeCarriageReturn(IOUtils.toString(resourceAsStream, StandardCharsets.UTF_8));
        ObjectNode zenodoDeposit = DarkTaxonUtil.toPhysicalObjectDeposit(jsonString, getPublicationDateFactory(), getTestConfig());
        String actual = zenodoDeposit.toPrettyString();

        assertThat(actual, Is.is(IOUtils.toString(getClass().getResourceAsStream("darktaxon/occurrence-zenodo.json"), StandardCharsets.UTF_8)));
    }

    @Test
    public void physicalObjectDepositWithKeyImage() throws IOException, MissingMetadataFieldException {
        InputStream resourceAsStream = getClass().getResourceAsStream("darktaxon/occurrence-with-key-image.json");
        assertNotNull(resourceAsStream);
        String jsonString = TestUtil.removeCarriageReturn(IOUtils.toString(resourceAsStream, StandardCharsets.UTF_8));
        ObjectNode zenodoDeposit = DarkTaxonUtil.toPhysicalObjectDeposit(jsonString, getPublicationDateFactory(), getTestConfig());
        String actual = zenodoDeposit.toPrettyString();
        System.out.println(actual);

        assertThat(actual, Is.is(IOUtils.toString(getClass().getResourceAsStream("darktaxon/occurrence-with-key-image-zenodo.json"), StandardCharsets.UTF_8)));
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
