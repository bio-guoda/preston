package bio.guoda.preston.cmd;

import bio.guoda.preston.HashType;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.HashKeyUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static junit.framework.TestCase.assertNotNull;
import static org.hamcrest.MatcherAssert.assertThat;

public class DarkTaxonRecordTranslatorTest {

    @Test
    public void photoDeposit() throws IOException {
        JsonNode multimedia = new ObjectMapper().readTree(getClass().getResourceAsStream("darktaxon/multimedia.json"));
        assertNotNull(multimedia);

        JsonNode jsonNode = multimedia.get("http://purl.org/dc/terms/identifier");
        String filename = jsonNode.asText();
        JsonNode specimenReference = multimedia.get("http://rs.tdwg.org/ac/terms/associatedSpecimenReference");
        String specimenId = specimenReference.asText();
        JsonNode hashValue = multimedia.get("http://rs.tdwg.org/ac/terms/hashValue");
        JsonNode hashAlgo = multimedia.get("http://rs.tdwg.org/ac/terms/hashFunction");
        HashType hashType = HashType.valueOf(StringUtils.lowerCase(hashAlgo.asText()));
        IRI imageContentId = RefNodeFactory.toIRI(hashType.getPrefix() + hashValue.asText());
        if (!HashKeyUtil.isValidHashKey(imageContentId)) {
            throw new IllegalArgumentException("unsupported content id [" + imageContentId + "]");
        }

        JsonNode format = multimedia.get("http://purl.org/dc/elements/1.1/format");


        ObjectNode zenodoMetadata = new ObjectMapper().createObjectNode();

        String title = multimedia.get("http://purl.org/dc/terms/title").asText();
        String description = multimedia.get("http://purl.org/dc/terms/description").asText();
        DarkTaxonUtil.populatePhotoDepositMetadata(
                zenodoMetadata,
                filename,
                specimenId,
                imageContentId.getIRIString(),
                "image/" + format.asText(),
                new PublicationDateFactory() {
                    @Override
                    public String getPublicationDate() {
                        return "1999-12-31";
                    }
                },
                Arrays.asList("mfn-test"),
                title,
                description
        );
        ZenodoMetaUtil.addCustomField(zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_AC_RESOURCE_CAPTURE_DEVICE, multimedia.get("http://rs.tdwg.org/ac/terms/captureDevice").asText());
        ZenodoMetaUtil.addCustomField(zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_AC_RESOURCE_CREATION_TECHNIQUE, multimedia.get("http://rs.tdwg.org/ac/terms/resourceCreationTechnique").asText());
        String[] split = StringUtils.split(specimenId, ":");
        String catalogNumber = split.length > 0 ? split[split.length-1] : specimenId;
        ZenodoMetaUtil.addCustomField(zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_CATALOG_NUMBER, catalogNumber);




        String actual = ZenodoMetaUtil.wrap(zenodoMetadata).toPrettyString();

        System.out.println(actual);

        assertThat(actual, Is.is(IOUtils.toString(getClass().getResourceAsStream("darktaxon/multimedia-zenodo.json"), StandardCharsets.UTF_8)));

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
