package bio.guoda.preston.cmd;

import bio.guoda.preston.DateUtil;
import bio.guoda.preston.HashType;
import bio.guoda.preston.Hasher;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.HashKeyUtil;
import bio.guoda.preston.store.TestUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static bio.guoda.preston.cmd.ZenodoMetaUtil.PUBLICATION_DATE;
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
        InputStream resourceAsStream = getClass().getResourceAsStream("darktaxon/event.json");
        String jsonString = TestUtil.removeCarriageReturn(IOUtils.toString(resourceAsStream, StandardCharsets.UTF_8));
        JsonNode multimedia = new ObjectMapper().readTree(jsonString);
        assertNotNull(multimedia);

        ObjectNode zenodoMetadata = new ObjectMapper().createObjectNode();


        String eventId = multimedia.get("http://rs.tdwg.org/dwc/terms/eventID").asText();
        String protocol = multimedia.get("http://rs.tdwg.org/dwc/terms/samplingProtocol").asText();
        String locality = multimedia.get("http://rs.tdwg.org/dwc/terms/locality").asText();
        String eventDate = multimedia.get("http://rs.tdwg.org/dwc/terms/eventDate").asText();
        String title = "Sample event at " + locality + " on " + eventDate + " using " + protocol;
        zenodoMetadata.put(ZenodoMetaUtil.TITLE, title);

        DarkTaxonUtil.setDescription(zenodoMetadata, title);

        ZenodoMetaUtil.addCustomField(zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_INSTITUTION_CODE, "MfN");

        ZenodoMetaUtil.addCustomField(zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_EVENT_DATE, StringUtils.split(eventDate, "/")[0]);
        ZenodoMetaUtil.addCustomField(zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_VERBATIM_EVENT_DATE, eventDate);
        ZenodoMetaUtil.addCustomField(zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_LOCALITY, locality);
        ZenodoMetaUtil.addCustomField(zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_RECORDED_BY, multimedia.get("http://rs.tdwg.org/dwc/terms/recordedBy").asText());
        ZenodoMetaUtil.addCustomField(zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_RECORDED_BY_ID, multimedia.get("http://rs.tdwg.org/dwc/terms/recordedByID").asText());
        ZenodoMetaUtil.addCustomField(zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_GBIF_DWC_RECORDED_BY_ID, multimedia.get("http://rs.tdwg.org/dwc/terms/recordedByID").asText());
        ZenodoMetaUtil.setFilename(zenodoMetadata, "event.json");
        ZenodoMetaUtil.appendIdentifier(zenodoMetadata, ZenodoMetaUtil.IS_ALTERNATE_IDENTIFIER, eventId);

        DarkTaxonUtil.appendAlternateIdentifiers(zenodoMetadata, Hasher.calcHashIRI(jsonString, HashType.md5).getIRIString());
        ZenodoMetaUtil.setValue(zenodoMetadata, ZenodoMetaUtil.UPLOAD_TYPE, ZenodoMetaUtil.UPLOAD_TYPE_EVENT);
        ZenodoMetaUtil.setCreators(zenodoMetadata, Arrays.asList("Museum für Naturkunde Berlin"));
        ZenodoMetaUtil.setValue(zenodoMetadata, PUBLICATION_DATE, new PublicationDateFactory() {
                    @Override
                    public String getPublicationDate() {
                        return "1999-12-31";
                    }
                }.getPublicationDate());
        ZenodoMetaUtil.setCommunities(zenodoMetadata, Arrays.asList("mfn-test").stream());
        addReferences(zenodoMetadata);


        String actual = ZenodoMetaUtil.wrap(zenodoMetadata).toPrettyString();

        System.out.println(actual);

        assertThat(actual, Is.is(IOUtils.toString(getClass().getResourceAsStream("darktaxon/event-zenodo.json"), StandardCharsets.UTF_8)));


    }

    private void addReferences(ObjectNode zenodoMetadata) {
        ZenodoMetaUtil.append(zenodoMetadata, ZenodoMetaUtil.REFERENCES, "Hartop E, Srivathsan A, Ronquist F, Meier R (2022) Towards Large-Scale Integrative Taxonomy (LIT): resolving the data conundrum for dark taxa. Syst Biol 71:1404–1422. https://doi.org/10.1093/sysbio/syac033");
        ZenodoMetaUtil.append(zenodoMetadata, ZenodoMetaUtil.REFERENCES, "Srivathsan, A., Meier, R. (2024). Scalable, Cost-Effective, and Decentralized DNA Barcoding with Oxford Nanopore Sequencing. In: DeSalle, R. (eds) DNA Barcoding. Methods in Molecular Biology, vol 2744. Humana, New York, NY. https://doi.org/10.1007/978-1-0716-3581-0_14");
    }

    @Test
    public void physicalObjectDeposit() throws IOException {
        InputStream resourceAsStream = getClass().getResourceAsStream("darktaxon/occurrence.json");
        String jsonString = TestUtil.removeCarriageReturn(IOUtils.toString(resourceAsStream, StandardCharsets.UTF_8));
        JsonNode multimedia = new ObjectMapper().readTree(jsonString);
        assertNotNull(multimedia);

        ObjectNode zenodoMetadata = new ObjectMapper().createObjectNode();

        String eventId = multimedia.get("http://rs.tdwg.org/dwc/terms/eventID").asText();
        String occurrenceId = multimedia.get("http://rs.tdwg.org/dwc/terms/occurrenceID").asText();
        String country = multimedia.get("http://rs.tdwg.org/dwc/terms/country").asText();
        String eventDate = multimedia.get("http://rs.tdwg.org/dwc/terms/eventDate").asText();
        String title = "Physical object " + occurrenceId + " sampled through event " + eventId + " on " + eventDate;
        zenodoMetadata.put(ZenodoMetaUtil.TITLE, title);

        DarkTaxonUtil.setDescription(zenodoMetadata, title);

        ZenodoMetaUtil.addCustomField(zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_INSTITUTION_CODE, "MfN");

        ZenodoMetaUtil.addCustomField(zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_EVENT_DATE, StringUtils.split(eventDate, "/")[0]);
        ZenodoMetaUtil.addCustomField(zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_VERBATIM_EVENT_DATE, eventDate);
        ZenodoMetaUtil.addCustomField(zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_COUNTRY, country);
        ZenodoMetaUtil.addCustomField(zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_CATALOG_NUMBER, multimedia.get("http://rs.tdwg.org/dwc/terms/catalogNumber").asText());
        ZenodoMetaUtil.addCustomField(zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_BASIS_OF_RECORD, multimedia.get("http://rs.tdwg.org/dwc/terms/basisOfRecord").asText());
        ZenodoMetaUtil.addCustomField(zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_SCIENTIFIC_NAME, multimedia.get("http://rs.tdwg.org/dwc/terms/scientificName").asText());
        ZenodoMetaUtil.addCustomField(zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_MATERIAL_SAMPLE_ID, occurrenceId);
        ZenodoMetaUtil.setFilename(zenodoMetadata, "event.json");
        ZenodoMetaUtil.appendIdentifier(zenodoMetadata, ZenodoMetaUtil.IS_ALTERNATE_IDENTIFIER, occurrenceId);
        ZenodoMetaUtil.appendIdentifier(zenodoMetadata, ZenodoMetaUtil.IS_DERIVED_FROM, eventId);

        DarkTaxonUtil.appendAlternateIdentifiers(zenodoMetadata, Hasher.calcHashIRI(jsonString, HashType.md5).getIRIString());
        ZenodoMetaUtil.setValue(zenodoMetadata, ZenodoMetaUtil.UPLOAD_TYPE, ZenodoMetaUtil.UPLOAD_TYPE_PHYSICAL_OBJECT);
        ZenodoMetaUtil.setCreators(zenodoMetadata, Arrays.asList("Museum für Naturkunde Berlin"));
        ZenodoMetaUtil.setValue(zenodoMetadata, PUBLICATION_DATE, new PublicationDateFactory() {
            @Override
            public String getPublicationDate() {
                return "1999-12-31";
            }
        }.getPublicationDate());
        ZenodoMetaUtil.setCommunities(zenodoMetadata, Arrays.asList("mfn-test").stream());
        addReferences(zenodoMetadata);


        String actual = ZenodoMetaUtil.wrap(zenodoMetadata).toPrettyString();

        System.out.println(actual);

        assertThat(actual, Is.is(IOUtils.toString(getClass().getResourceAsStream("darktaxon/occurrence-zenodo.json"), StandardCharsets.UTF_8)));
    }

}
