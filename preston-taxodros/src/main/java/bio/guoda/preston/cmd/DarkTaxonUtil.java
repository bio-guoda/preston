package bio.guoda.preston.cmd;

import bio.guoda.preston.HashType;
import bio.guoda.preston.Hasher;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.HashKeyUtil;
import bio.guoda.preston.zenodo.ZenodoConfig;
import bio.guoda.preston.zenodo.ZenodoUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static bio.guoda.preston.cmd.ZenodoMetaUtil.PUBLICATION_DATE;

public class DarkTaxonUtil {
    private static final Logger LOG = LoggerFactory.getLogger(DarkTaxonUtil.class);
    public static final String LSID_PREFIX = "urn:lsid:github.com:darktaxon:";
    public static final String DC_TERMS_DYNAMIC_PROPERTIES = "http://rs.tdwg.org/dwc/terms/dynamicProperties";

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

    public static ObjectNode toPhotoDeposit(JsonNode multimediaRecord, PublicationDateFactory publicationDateFactory, ZenodoConfig ctx) throws MissingMetadataFieldException {

        String formatText = getValueOrThrow(multimediaRecord, "http://purl.org/dc/elements/1.1/format");

        String filename = getValueOrThrow(multimediaRecord, "http://purl.org/dc/terms/identifier");
        if (!filename.contains(".")) {
            filename = filename + "." + StringUtils.lowerCase(formatText);
        }

        String specimenId = getValueOrThrow(multimediaRecord, "http://rs.tdwg.org/ac/terms/associatedSpecimenReference");
        String hash = getValueOrThrow(multimediaRecord, "http://rs.tdwg.org/ac/terms/hashValue");
        String hashAlgoText = getValueOrThrow(multimediaRecord, "http://rs.tdwg.org/ac/terms/hashFunction");
        HashType hashType = HashType.valueOf(StringUtils.lowerCase(hashAlgoText));
        IRI imageContentId = RefNodeFactory.toIRI(hashType.getPrefix() + hash);
        if (!HashKeyUtil.isValidHashKey(imageContentId)) {
            throw new IllegalArgumentException("unsupported content id [" + imageContentId + "]");
        }

        ObjectNode zenodoMetadata = new ObjectMapper().createObjectNode();

        JsonNode tagNode = multimediaRecord.get("http://rs.tdwg.org/ac/terms/tag");
        if (tagNode != null) {
            String[] tags = StringUtils.split(tagNode.asText(), "|");
            Stream.of(tags).forEach(tag -> ZenodoMetaUtil.addKeyword(zenodoMetadata, StringUtils.trim(tag)));
        }

        String title = getValueOrThrow(multimediaRecord, "http://purl.org/dc/terms/title");
        String description = getValueOrThrow(multimediaRecord, "http://purl.org/dc/terms/description");

        JsonNode creditNode = multimediaRecord.get("http://ns.adobe.com/photoshop/1.0/Credit");
        if (creditNode != null && !creditNode.isNull()) {
            description = description + "\n\n" + creditNode.asText();
        }

        JsonNode creatorNode = multimediaRecord.get("http://purl.org/dc/elements/1.1/creator");
        List<String> creators = Arrays.asList(creatorNode == null || creatorNode.isNull() ? "Museum für Naturkunde Berlin" : creatorNode.asText());
        populatePhotoDepositMetadata(
                zenodoMetadata,
                filename,
                specimenId,
                imageContentId.getIRIString(),
                "image/" + formatText,
                publicationDateFactory,
                ctx.getCommunities(),
                title,
                description,
                creators
        );
        String serviceAccessPoint = getValueOrThrow(multimediaRecord, "http://rs.tdwg.org/ac/terms/hasServiceAccessPoint");
        ZenodoMetaUtil.appendIdentifier(zenodoMetadata, ZenodoMetaUtil.IS_DERIVED_FROM, ZenodoUtils.getSearchPageForExistingDepositions(ctx, Arrays.asList(specimenId)).getIRIString());
        ZenodoMetaUtil.appendIdentifier(zenodoMetadata, ZenodoMetaUtil.IS_DERIVED_FROM, ZenodoUtils.getQueryForExistingRecords(ctx, Arrays.asList(specimenId)).getIRIString());
        ZenodoMetaUtil.appendIdentifier(zenodoMetadata, ZenodoMetaUtil.IS_DOCUMENTED_BY, serviceAccessPoint);
        addFieldValueAsZenodoCustomFieldIfAvailable(multimediaRecord, zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_AC_SUBJECT_PART, "http://rs.tdwg.org/ac/terms/subjectPart");
        addFieldValueAsZenodoCustomFieldIfAvailable(multimediaRecord, zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_AC_CAPTURE_DEVICE, "http://rs.tdwg.org/ac/terms/captureDevice");
        addFieldValueAsZenodoCustomFieldIfAvailable(multimediaRecord, zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_AC_RESOURCE_CREATION_TECHNIQUE, "http://rs.tdwg.org/ac/terms/resourceCreationTechnique");
        String[] split = StringUtils.split(specimenId, ":");
        String catalogNumber = split.length > 0 ? split[split.length - 1] : specimenId;
        addValueAsCustomFieldIfAvailable(zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_CATALOG_NUMBER, catalogNumber);

        return ZenodoMetaUtil.wrap(zenodoMetadata);
    }

    private static String getValueOrThrow(JsonNode multimediaRecord, String fieldName) throws MissingMetadataFieldException {
        JsonNode node = multimediaRecord.get(fieldName);
        if (node == null || node.isNull() || StringUtils.isBlank(node.asText())) {
            throw new MissingMetadataFieldException("no value specified for [" + fieldName + "] in [" + multimediaRecord.toPrettyString() + "]");
        }
        return StringUtils.trim(node.asText());
    }

    static ObjectNode toEventDeposit(String jsonString, PublicationDateFactory publicationDateFactory, ZenodoConfig ctx) throws JsonProcessingException, MissingMetadataFieldException {
        JsonNode multimedia = new ObjectMapper().readTree(jsonString);
        ObjectNode zenodoMetadata = new ObjectMapper().createObjectNode();


        String eventId = getValueOrThrow(multimedia, "http://rs.tdwg.org/dwc/terms/eventID");
        String protocol = getValueOrThrow(multimedia, "http://rs.tdwg.org/dwc/terms/samplingProtocol");
        String locality = getValueOrThrow(multimedia, "http://rs.tdwg.org/dwc/terms/locality");
        String eventDate = getValueOrThrow(multimedia, "http://rs.tdwg.org/dwc/terms/eventDate");
        String title = "Sample event at " + locality + " on " + eventDate + " using " + protocol;
        zenodoMetadata.put(ZenodoMetaUtil.TITLE, title);

        setDescription(zenodoMetadata, title);

        addValueAsCustomFieldIfAvailable(zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_INSTITUTION_CODE, "MfN");

        addValueAsCustomFieldIfAvailable(zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_EVENT_DATE, StringUtils.split(eventDate, "/")[0]);
        addValueAsCustomFieldIfAvailable(zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_VERBATIM_EVENT_DATE, eventDate);
        addValueAsCustomFieldIfAvailable(zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_LOCALITY, locality);
        addValueAsCustomFieldIfAvailable(zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_RECORDED_BY, getValueOrThrow(multimedia, "http://rs.tdwg.org/dwc/terms/recordedBy"));
        addValueAsCustomFieldIfAvailable(zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_RECORDED_BY_ID, getValueOrThrow(multimedia, "http://rs.tdwg.org/dwc/terms/recordedByID"));
        addValueAsCustomFieldIfAvailable(zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_GBIF_DWC_RECORDED_BY_ID, getValueOrThrow(multimedia, "http://rs.tdwg.org/dwc/terms/recordedByID"));
        ZenodoMetaUtil.setFilename(zenodoMetadata, "event.json");
        ZenodoMetaUtil.appendIdentifier(zenodoMetadata, ZenodoMetaUtil.IS_ALTERNATE_IDENTIFIER, eventId);
        ZenodoMetaUtil.appendIdentifier(zenodoMetadata, ZenodoMetaUtil.IS_VERSION_OF, ZenodoUtils.getSearchPageForExistingDepositions(ctx, Arrays.asList(eventId)).getIRIString());
        ZenodoMetaUtil.appendIdentifier(zenodoMetadata, ZenodoMetaUtil.IS_VERSION_OF, ZenodoUtils.getQueryForExistingRecords(ctx, Arrays.asList(eventId)).getIRIString());

        appendAlternateIdentifiers(zenodoMetadata, Hasher.calcHashIRI(jsonString, HashType.md5).getIRIString());
        ZenodoMetaUtil.setValue(zenodoMetadata, ZenodoMetaUtil.UPLOAD_TYPE, ZenodoMetaUtil.UPLOAD_TYPE_EVENT);
        ZenodoMetaUtil.setCreators(zenodoMetadata, Arrays.asList("Museum für Naturkunde Berlin"));
        ZenodoMetaUtil.setValue(zenodoMetadata, PUBLICATION_DATE, publicationDateFactory.getPublicationDate());
        ZenodoMetaUtil.setCommunities(zenodoMetadata, ctx.getCommunities().stream());
        addReferences(zenodoMetadata);


        return ZenodoMetaUtil.wrap(zenodoMetadata);
    }

    private static void addReferences(ObjectNode zenodoMetadata) {
        ZenodoMetaUtil.append(zenodoMetadata, ZenodoMetaUtil.REFERENCES, "Hartop E, Srivathsan A, Ronquist F, Meier R (2022) Towards Large-Scale Integrative Taxonomy (LIT): resolving the data conundrum for dark taxa. Syst Biol 71:1404–1422. https://doi.org/10.1093/sysbio/syac033");
        ZenodoMetaUtil.append(zenodoMetadata, ZenodoMetaUtil.REFERENCES, "Srivathsan, A., Meier, R. (2024). Scalable, Cost-Effective, and Decentralized DNA Barcoding with Oxford Nanopore Sequencing. In: DeSalle, R. (eds) DNA Barcoding. Methods in Molecular Biology, vol 2744. Humana, New York, NY. https://doi.org/10.1007/978-1-0716-3581-0_14");
    }

    public static ObjectNode toPhysicalObjectDeposit(String jsonString, PublicationDateFactory publicationDateFactory, ZenodoConfig ctx) throws JsonProcessingException, MissingMetadataFieldException {
        JsonNode multimedia = new ObjectMapper().readTree(jsonString);

        ObjectNode zenodoMetadata = new ObjectMapper().createObjectNode();

        String eventId = getValueOrThrow(multimedia, "http://rs.tdwg.org/dwc/terms/eventID");
        String occurrenceId = getValueOrThrow(multimedia, "http://rs.tdwg.org/dwc/terms/occurrenceID");
        String country = getValueOrThrow(multimedia, "http://rs.tdwg.org/dwc/terms/country");
        String eventDate = getValueOrThrow(multimedia, "http://rs.tdwg.org/dwc/terms/eventDate");
        String title = "Physical object " + occurrenceId + " sampled through event " + eventId + " on " + eventDate;
        zenodoMetadata.put(ZenodoMetaUtil.TITLE, title);

        setDescription(zenodoMetadata, title);

        addValueAsCustomFieldIfAvailable(zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_INSTITUTION_CODE, "MfN");

        addValueAsCustomFieldIfAvailable(zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_EVENT_DATE, StringUtils.split(eventDate, "/")[0]);
        addValueAsCustomFieldIfAvailable(zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_VERBATIM_EVENT_DATE, eventDate);
        addValueAsCustomFieldIfAvailable(zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_COUNTRY, country);
        addFieldValueAsZenodoCustomFieldIfAvailable(multimedia, zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_CATALOG_NUMBER, "http://rs.tdwg.org/dwc/terms/catalogNumber");
        addFieldValueAsZenodoCustomFieldIfAvailable(multimedia, zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_BASIS_OF_RECORD, "http://rs.tdwg.org/dwc/terms/basisOfRecord");
        addFieldValueAsZenodoCustomFieldIfAvailable(multimedia, zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_SCIENTIFIC_NAME, "http://rs.tdwg.org/dwc/terms/scientificName");
        addValueAsCustomFieldIfAvailable(zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_MATERIAL_SAMPLE_ID, occurrenceId);
        String filename = "event.json";
        String contentId = null;

        if (multimedia.has(DC_TERMS_DYNAMIC_PROPERTIES)) {
            String dynamicProperties = multimedia.get(DC_TERMS_DYNAMIC_PROPERTIES).asText();
            try {
                JsonNode properties = new ObjectMapper().readTree(dynamicProperties);
                JsonNode at = properties.at("/keyImageFilename");
                if (!at.isMissingNode()) {
                    filename = at.asText();
                }
                JsonNode id = properties.at("/keyImageId");
                if (!id.isMissingNode()) {
                    String contentIdCandidate = id.asText();
                    if (HashKeyUtil.hashTypeFor(contentIdCandidate) != null) {
                        contentId = HashKeyUtil.extractContentHash(RefNodeFactory.toIRI(contentIdCandidate)).getIRIString();
                    }
                }
            } catch (JsonProcessingException e) {
                LOG.warn("found dynamic properties, but failed to process [" + dynamicProperties + "]", e);
            }
        }

        if (StringUtils.isBlank(contentId)) {
            contentId = Hasher.calcHashIRI(jsonString, HashType.md5).getIRIString();
        }

        ZenodoMetaUtil.setFilename(zenodoMetadata, filename);
        ZenodoMetaUtil.appendIdentifier(zenodoMetadata, ZenodoMetaUtil.IS_ALTERNATE_IDENTIFIER, occurrenceId);
        ZenodoMetaUtil.appendIdentifier(zenodoMetadata, ZenodoMetaUtil.IS_VERSION_OF, ZenodoUtils.getSearchPageForExistingDepositions(ctx, Arrays.asList(occurrenceId)).getIRIString());
        ZenodoMetaUtil.appendIdentifier(zenodoMetadata, ZenodoMetaUtil.IS_VERSION_OF, ZenodoUtils.getQueryForExistingRecords(ctx, Arrays.asList(occurrenceId)).getIRIString());
        ZenodoMetaUtil.appendIdentifier(zenodoMetadata, ZenodoMetaUtil.IS_DERIVED_FROM, eventId);
        ZenodoMetaUtil.appendIdentifier(zenodoMetadata, ZenodoMetaUtil.IS_DERIVED_FROM, ZenodoUtils.getSearchPageForExistingDepositions(ctx, Arrays.asList(eventId)).getIRIString());
        ZenodoMetaUtil.appendIdentifier(zenodoMetadata, ZenodoMetaUtil.IS_DERIVED_FROM, ZenodoUtils.getQueryForExistingRecords(ctx, Arrays.asList(eventId)).getIRIString());

        appendAlternateIdentifiers(zenodoMetadata, contentId);
        ZenodoMetaUtil.setValue(zenodoMetadata, ZenodoMetaUtil.UPLOAD_TYPE, ZenodoMetaUtil.UPLOAD_TYPE_PHYSICAL_OBJECT);
        ZenodoMetaUtil.setCreators(zenodoMetadata, Arrays.asList("Museum für Naturkunde Berlin"));
        ZenodoMetaUtil.setValue(zenodoMetadata, PUBLICATION_DATE, publicationDateFactory.getPublicationDate());
        ZenodoMetaUtil.setCommunities(zenodoMetadata, ctx.getCommunities().stream());
        addReferences(zenodoMetadata);

        return ZenodoMetaUtil.wrap(zenodoMetadata);
    }

    private static void addValueAsCustomFieldIfAvailable(ObjectNode zenodoMetadata, String customField, String value) {
        if (StringUtils.isNotBlank(value)) {
            ZenodoMetaUtil.addCustomField(zenodoMetadata, customField, value);
        }
    }

    private static void addFieldValueAsZenodoCustomFieldIfAvailable(JsonNode multimedia, ObjectNode zenodoMetadata, String customField, String fieldName) {
        if (multimedia.has(fieldName)) {
            JsonNode valueNode = multimedia.get(fieldName);
            if (!valueNode.isNull() && StringUtils.isNotBlank(valueNode.asText())) {
                ZenodoMetaUtil.addCustomField(zenodoMetadata, customField, valueNode.asText());
            }
        }
    }
}
