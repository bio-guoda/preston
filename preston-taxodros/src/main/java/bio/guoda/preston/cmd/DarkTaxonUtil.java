package bio.guoda.preston.cmd;

import bio.guoda.preston.HashType;
import bio.guoda.preston.Hasher;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.HashKeyUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static bio.guoda.preston.cmd.ZenodoMetaUtil.PUBLICATION_DATE;

public class DarkTaxonUtil {
    public static final String LSID_PREFIX = "urn:lsid:github.com:darktaxon:";

    static void appendAlternateIdentifiers(ObjectNode linkRecords, String imageContentId) {
        ZenodoMetaUtil.appendIdentifier(linkRecords, ZenodoMetaUtil.IS_ALTERNATE_IDENTIFIER, imageContentId);
        ZenodoMetaUtil.appendIdentifier(linkRecords, ZenodoMetaUtil.HAS_VERSION, imageContentId);
    }

    public static void populatePhotoDepositMetadata(ObjectNode objectNode, String imageFilename, String specimenId, String imageContentId, String mimeType, PublicationDateFactory publicationDateFactory, List<String> communities, String title, String description, List<String> creators) {
        ZenodoMetaUtil.addCustomField(objectNode, ZenodoMetaUtil.FIELD_CUSTOM_DWC_CATALOG_NUMBER, specimenId);
        ZenodoMetaUtil.addCustomField(objectNode, ZenodoMetaUtil.FIELD_CUSTOM_DWC_MATERIAL_SAMPLE_ID, specimenId);
        ZenodoMetaUtil.addCustomField(objectNode, ZenodoMetaUtil.FIELD_CUSTOM_AC_ASSOCIATED_SPECIMEN, specimenId);
        objectNode.put(ZenodoMetaUtil.TITLE, title);
        setDescription(objectNode, description);
        ZenodoMetaUtil.setFilename(objectNode, imageFilename);
        appendAlternateIdentifiers(objectNode, imageContentId);
        String specimenLSID = StringUtils.startsWith(specimenId, "urn:lsid:") ? specimenId : LSID_PREFIX + specimenId;
        ZenodoMetaUtil.appendIdentifier(objectNode, ZenodoMetaUtil.IS_ALTERNATE_IDENTIFIER, specimenLSID + ":" + imageFilename);
        ZenodoMetaUtil.appendIdentifier(objectNode, ZenodoMetaUtil.IS_DERIVED_FROM, specimenLSID);
        ZenodoMetaUtil.setType(objectNode, mimeType);
        ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.UPLOAD_TYPE, ZenodoMetaUtil.UPLOAD_TYPE_IMAGE);
        ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.IMAGE_TYPE, ZenodoMetaUtil.IMAGE_TYPE_PHOTO);
        ZenodoMetaUtil.setCreators(objectNode, creators);
        ZenodoMetaUtil.setValue(objectNode, PUBLICATION_DATE, publicationDateFactory.getPublicationDate());
        ZenodoMetaUtil.setCommunities(objectNode, communities.stream());
    }

    public static void setDescription(ObjectNode objectNode, String description) {
        objectNode.put("description", description);
    }

    public static ObjectNode toPhotoDeposit(JsonNode multimediaRecord, PublicationDateFactory publicationDateFactory, List<String> communities) {
        JsonNode format = multimediaRecord.get("http://purl.org/dc/elements/1.1/format");
        JsonNode jsonNode = multimediaRecord.get("http://purl.org/dc/terms/identifier");
        String filename = jsonNode.asText();
        if (!filename.contains(".")) {
            filename = filename + "." + StringUtils.lowerCase(format.asText());
        }

        JsonNode specimenReference = multimediaRecord.get("http://rs.tdwg.org/ac/terms/associatedSpecimenReference");
        String specimenId = specimenReference.asText();
        JsonNode hashValue = multimediaRecord.get("http://rs.tdwg.org/ac/terms/hashValue");
        JsonNode hashAlgo = multimediaRecord.get("http://rs.tdwg.org/ac/terms/hashFunction");
        HashType hashType = HashType.valueOf(StringUtils.lowerCase(hashAlgo.asText()));
        IRI imageContentId = RefNodeFactory.toIRI(hashType.getPrefix() + hashValue.asText());
        if (!HashKeyUtil.isValidHashKey(imageContentId)) {
            throw new IllegalArgumentException("unsupported content id [" + imageContentId + "]");
        }

        ObjectNode zenodoMetadata = new ObjectMapper().createObjectNode();

        JsonNode tagNode = multimediaRecord.get("http://rs.tdwg.org/ac/terms/tag");
        if (tagNode != null) {
            String[] tags = StringUtils.split(tagNode.asText(), "|");
            Stream.of(tags).forEach(tag -> ZenodoMetaUtil.addKeyword(zenodoMetadata, StringUtils.trim(tag)));
        }

        String title = multimediaRecord.get("http://purl.org/dc/terms/title").asText();
        String description = multimediaRecord.get("http://purl.org/dc/terms/description").asText();

        JsonNode creditNode = multimediaRecord.get("http://ns.adobe.com/photoshop/1.0/Credit");
        if (creditNode != null) {
            description = description + "\n\n" + creditNode.asText();
        }

        JsonNode creatorNode = multimediaRecord.get("http://purl.org/dc/elements/1.1/creator");
        List<String> creators = Arrays.asList(creatorNode == null || creatorNode.isNull() ? "Museum für Naturkunde Berlin" : creatorNode.asText());
        populatePhotoDepositMetadata(
                zenodoMetadata,
                filename,
                specimenId,
                imageContentId.getIRIString(),
                "image/" + format.asText(),
                publicationDateFactory,
                communities,
                title,
                description,
                creators
        );
        ZenodoMetaUtil.appendIdentifier(zenodoMetadata, ZenodoMetaUtil.IS_DOCUMENTED_BY, multimediaRecord.get("http://rs.tdwg.org/ac/terms/hasServiceAccessPoint").asText());
        ZenodoMetaUtil.addCustomField(zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_AC_SUBJECT_PART, multimediaRecord.get("http://rs.tdwg.org/ac/terms/subjectPart").asText());
        ZenodoMetaUtil.addCustomField(zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_AC_CAPTURE_DEVICE, multimediaRecord.get("http://rs.tdwg.org/ac/terms/captureDevice").asText());
        ZenodoMetaUtil.addCustomField(zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_AC_RESOURCE_CREATION_TECHNIQUE, multimediaRecord.get("http://rs.tdwg.org/ac/terms/resourceCreationTechnique").asText());
        String[] split = StringUtils.split(specimenId, ":");
        String catalogNumber = split.length > 0 ? split[split.length - 1] : specimenId;
        ZenodoMetaUtil.addCustomField(zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_CATALOG_NUMBER, catalogNumber);

        return ZenodoMetaUtil.wrap(zenodoMetadata);
    }

    static ObjectNode toEventDeposit(String jsonString, PublicationDateFactory publicationDateFactory, List<String> communities) throws JsonProcessingException {
        JsonNode multimedia = new ObjectMapper().readTree(jsonString);
        ObjectNode zenodoMetadata = new ObjectMapper().createObjectNode();


        String eventId = multimedia.get("http://rs.tdwg.org/dwc/terms/eventID").asText();
        String protocol = multimedia.get("http://rs.tdwg.org/dwc/terms/samplingProtocol").asText();
        String locality = multimedia.get("http://rs.tdwg.org/dwc/terms/locality").asText();
        String eventDate = multimedia.get("http://rs.tdwg.org/dwc/terms/eventDate").asText();
        String title = "Sample event at " + locality + " on " + eventDate + " using " + protocol;
        zenodoMetadata.put(ZenodoMetaUtil.TITLE, title);

        setDescription(zenodoMetadata, title);

        ZenodoMetaUtil.addCustomField(zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_INSTITUTION_CODE, "MfN");

        ZenodoMetaUtil.addCustomField(zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_EVENT_DATE, StringUtils.split(eventDate, "/")[0]);
        ZenodoMetaUtil.addCustomField(zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_VERBATIM_EVENT_DATE, eventDate);
        ZenodoMetaUtil.addCustomField(zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_LOCALITY, locality);
        ZenodoMetaUtil.addCustomField(zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_RECORDED_BY, multimedia.get("http://rs.tdwg.org/dwc/terms/recordedBy").asText());
        ZenodoMetaUtil.addCustomField(zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_RECORDED_BY_ID, multimedia.get("http://rs.tdwg.org/dwc/terms/recordedByID").asText());
        ZenodoMetaUtil.addCustomField(zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_GBIF_DWC_RECORDED_BY_ID, multimedia.get("http://rs.tdwg.org/dwc/terms/recordedByID").asText());
        ZenodoMetaUtil.setFilename(zenodoMetadata, "event.json");
        ZenodoMetaUtil.appendIdentifier(zenodoMetadata, ZenodoMetaUtil.IS_ALTERNATE_IDENTIFIER, eventId);

        appendAlternateIdentifiers(zenodoMetadata, Hasher.calcHashIRI(jsonString, HashType.md5).getIRIString());
        ZenodoMetaUtil.setValue(zenodoMetadata, ZenodoMetaUtil.UPLOAD_TYPE, ZenodoMetaUtil.UPLOAD_TYPE_EVENT);
        ZenodoMetaUtil.setCreators(zenodoMetadata, Arrays.asList("Museum für Naturkunde Berlin"));
        ZenodoMetaUtil.setValue(zenodoMetadata, PUBLICATION_DATE, publicationDateFactory.getPublicationDate());
        ZenodoMetaUtil.setCommunities(zenodoMetadata, communities.stream());
        addReferences(zenodoMetadata);


        return ZenodoMetaUtil.wrap(zenodoMetadata);
    }

    private static void addReferences(ObjectNode zenodoMetadata) {
        ZenodoMetaUtil.append(zenodoMetadata, ZenodoMetaUtil.REFERENCES, "Hartop E, Srivathsan A, Ronquist F, Meier R (2022) Towards Large-Scale Integrative Taxonomy (LIT): resolving the data conundrum for dark taxa. Syst Biol 71:1404–1422. https://doi.org/10.1093/sysbio/syac033");
        ZenodoMetaUtil.append(zenodoMetadata, ZenodoMetaUtil.REFERENCES, "Srivathsan, A., Meier, R. (2024). Scalable, Cost-Effective, and Decentralized DNA Barcoding with Oxford Nanopore Sequencing. In: DeSalle, R. (eds) DNA Barcoding. Methods in Molecular Biology, vol 2744. Humana, New York, NY. https://doi.org/10.1007/978-1-0716-3581-0_14");
    }

    public static ObjectNode toPhysicalObjectDeposit(String jsonString, PublicationDateFactory publicationDateFactory, List<String> communities) throws JsonProcessingException {
        JsonNode multimedia = new ObjectMapper().readTree(jsonString);

        ObjectNode zenodoMetadata = new ObjectMapper().createObjectNode();

        String eventId = multimedia.get("http://rs.tdwg.org/dwc/terms/eventID").asText();
        String occurrenceId = multimedia.get("http://rs.tdwg.org/dwc/terms/occurrenceID").asText();
        String country = multimedia.get("http://rs.tdwg.org/dwc/terms/country").asText();
        String eventDate = multimedia.get("http://rs.tdwg.org/dwc/terms/eventDate").asText();
        String title = "Physical object " + occurrenceId + " sampled through event " + eventId + " on " + eventDate;
        zenodoMetadata.put(ZenodoMetaUtil.TITLE, title);

        setDescription(zenodoMetadata, title);

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

        appendAlternateIdentifiers(zenodoMetadata, Hasher.calcHashIRI(jsonString, HashType.md5).getIRIString());
        ZenodoMetaUtil.setValue(zenodoMetadata, ZenodoMetaUtil.UPLOAD_TYPE, ZenodoMetaUtil.UPLOAD_TYPE_PHYSICAL_OBJECT);
        ZenodoMetaUtil.setCreators(zenodoMetadata, Arrays.asList("Museum für Naturkunde Berlin"));
        ZenodoMetaUtil.setValue(zenodoMetadata, PUBLICATION_DATE, publicationDateFactory.getPublicationDate());
        ZenodoMetaUtil.setCommunities(zenodoMetadata, communities.stream());
        addReferences(zenodoMetadata);

        return ZenodoMetaUtil.wrap(zenodoMetadata);
    }
}
