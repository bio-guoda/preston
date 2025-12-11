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
import static bio.guoda.preston.cmd.ZenodoMetaUtil.RESOURCE_TYPE_PHOTO;

public class DarkTaxonUtil {
    private static final Logger LOG = LoggerFactory.getLogger(DarkTaxonUtil.class);
    public static final String LSID_PREFIX = "urn:lsid:github.com:darktaxon:";
    public static final String DC_TERMS_DYNAMIC_PROPERTIES = "http://rs.tdwg.org/dwc/terms/dynamicProperties";
    public static final String DWC_TERMS_RECORDED_BY_ID = "http://rs.tdwg.org/dwc/terms/recordedByID";
    public static final String EVENT = "event";
    public static final String PHYSICAL_OBJECT = "physicalobject";
    public static final String GBIF_RECORDED_BY_ID = "http://rs.gbif.org/terms/1.0/recordedByID";
    public static final String DWC_TERMS_EVENT_ID = "http://rs.tdwg.org/dwc/terms/eventID";
    public static final String DWC_TERMS_OCCURRENCE_ID = "http://rs.tdwg.org/dwc/terms/occurrenceID";
    public static final String DWC_TERMS_COUNTRY = "http://rs.tdwg.org/dwc/terms/country";
    public static final String DWC_TERMS_EVENT_DATE = "http://rs.tdwg.org/dwc/terms/eventDate";
    public static final String DWC_TERMS_SCIENTIFIC_NAME = "http://rs.tdwg.org/dwc/terms/scientificName";
    public static final String DWC_TERMS_BASIS_OF_RECORD = "http://rs.tdwg.org/dwc/terms/basisOfRecord";
    public static final String TERMS_CATALOG_NUMBER = "http://rs.tdwg.org/dwc/terms/catalogNumber";
    public static final String DWC_TERMS_LOCALITY = "http://rs.tdwg.org/dwc/terms/locality";
    public static final String DWC_TERMS_RECORDED_BY = "http://rs.tdwg.org/dwc/terms/recordedBy";
    public static final String DWC_TERMS_SAMPLING_PROTOCOL = "http://rs.tdwg.org/dwc/terms/samplingProtocol";
    public static final String AC_TERMS_SUBJECT_PART = "http://rs.tdwg.org/ac/terms/subjectPart";
    public static final String AC_TERMS_CAPTURE_DEVICE = "http://rs.tdwg.org/ac/terms/captureDevice";
    public static final String AC_TERMS_RESOURCE_CREATION_TECHNIQUE = "http://rs.tdwg.org/ac/terms/resourceCreationTechnique";
    public static final String AC_TERMS_ASSOCIATED_SPECIMEN_REFERENCE = "http://rs.tdwg.org/ac/terms/associatedSpecimenReference";
    public static final String AC_TERMS_HASH_VALUE = "http://rs.tdwg.org/ac/terms/hashValue";
    public static final String AC_TERMS_HASH_FUNCTION = "http://rs.tdwg.org/ac/terms/hashFunction";
    public static final String DC_TERMS_IDENTIFIER = "http://purl.org/dc/terms/identifier";
    public static final String DC_ELEMENTS_1_1_FORMAT = "http://purl.org/dc/elements/1.1/format";
    public static final String DWC_TEXT_COREID = "http://rs.tdwg.org/dwc/text/coreid";

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
        ZenodoMetaUtil.appendIdentifier(objectNode, ZenodoMetaUtil.IS_ALTERNATE_IDENTIFIER, specimenLSID + ":" + imageFilename, RESOURCE_TYPE_PHOTO);
        ZenodoMetaUtil.appendIdentifier(objectNode, ZenodoMetaUtil.DOCUMENTS, specimenLSID, PHYSICAL_OBJECT);
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

        String formatText = getValueOrThrow(multimediaRecord, DC_ELEMENTS_1_1_FORMAT);

        String filename = getValueOrThrow(multimediaRecord, DC_TERMS_IDENTIFIER);
        if (!filename.contains(".")) {
            filename = filename + "." + StringUtils.lowerCase(formatText);
        }

        String specimenId = getValueOrThrow(multimediaRecord, AC_TERMS_ASSOCIATED_SPECIMEN_REFERENCE);
        String hash = getValueOrThrow(multimediaRecord, AC_TERMS_HASH_VALUE);
        String hashAlgoText = getValueOrThrow(multimediaRecord, AC_TERMS_HASH_FUNCTION);
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

        addCustomFieldsIfAvailable(multimediaRecord, zenodoMetadata);
        ZenodoMetaUtil.appendIdentifier(zenodoMetadata, ZenodoMetaUtil.IS_VERSION_OF, ZenodoUtils.getSearchPageForExistingRecords(ctx, Arrays.asList(imageContentId.getIRIString()), RESOURCE_TYPE_PHOTO).getIRIString(), RESOURCE_TYPE_PHOTO);

        ZenodoMetaUtil.appendIdentifier(zenodoMetadata, ZenodoMetaUtil.DOCUMENTS, ZenodoUtils.getSearchPageForExistingRecords(ctx, Arrays.asList(specimenId), PHYSICAL_OBJECT).getIRIString(), PHYSICAL_OBJECT);
        if (multimediaRecord.has(DWC_TEXT_COREID)) {
            String eventId = multimediaRecord.get(DWC_TEXT_COREID).asText();
            ZenodoMetaUtil.appendIdentifier(zenodoMetadata, ZenodoMetaUtil.IS_DERIVED_FROM, ZenodoUtils.getSearchPageForExistingRecords(ctx, Arrays.asList(eventId), EVENT).getIRIString(), EVENT);
        }
        String[] split = StringUtils.split(specimenId, ":");
        String catalogNumber = split.length > 0 ? split[split.length - 1] : specimenId;
        addValueAsCustomFieldIfAvailable(zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_CATALOG_NUMBER, catalogNumber);

        return ZenodoMetaUtil.wrap(zenodoMetadata);
    }

    private static String getValueOrThrow(JsonNode multimediaRecord, String fieldName) throws MissingMetadataFieldException {
        if (!hasFieldValue(multimediaRecord, fieldName)) {
            throw new MissingMetadataFieldException("no value specified for [" + fieldName + "] in [" + multimediaRecord.toPrettyString() + "]");
        }

        JsonNode node = multimediaRecord.get(fieldName);
        return StringUtils.trim(node.asText());
    }

    static ObjectNode toEventDeposit(String jsonString, PublicationDateFactory publicationDateFactory, ZenodoConfig ctx) throws JsonProcessingException, MissingMetadataFieldException {
        JsonNode multimedia = new ObjectMapper().readTree(jsonString);
        ObjectNode zenodoMetadata = new ObjectMapper().createObjectNode();

        String eventId = getValueOrThrow(multimedia, DWC_TERMS_EVENT_ID);
        String protocol = getValueOrThrow(multimedia, DWC_TERMS_SAMPLING_PROTOCOL);
        String locality = getValueOrThrow(multimedia, DWC_TERMS_LOCALITY);
        String eventDate = getValueOrThrow(multimedia, DWC_TERMS_EVENT_DATE);
        String title = "Sample event at " + locality + " on " + eventDate + " using " + protocol;
        zenodoMetadata.put(ZenodoMetaUtil.TITLE, title);

        setDescription(zenodoMetadata, title);

        addCustomFieldsIfAvailable(multimedia, zenodoMetadata);
        appendKeyImageIfDefinedOrMetadataJSONOtherwise(jsonString, multimedia, zenodoMetadata, "event.json");

        ZenodoMetaUtil.appendIdentifier(zenodoMetadata, ZenodoMetaUtil.IS_ALTERNATE_IDENTIFIER, eventId, EVENT);
        ZenodoMetaUtil.appendIdentifier(zenodoMetadata, ZenodoMetaUtil.IS_VERSION_OF, ZenodoUtils.getSearchPageForExistingRecords(ctx, Arrays.asList(eventId), EVENT).getIRIString(), EVENT);

        ZenodoMetaUtil.setValue(zenodoMetadata, ZenodoMetaUtil.UPLOAD_TYPE, ZenodoMetaUtil.UPLOAD_TYPE_EVENT);
        ZenodoMetaUtil.setCreators(zenodoMetadata, Arrays.asList("Museum für Naturkunde Berlin"));
        ZenodoMetaUtil.setValue(zenodoMetadata, PUBLICATION_DATE, publicationDateFactory.getPublicationDate());
        ZenodoMetaUtil.setCommunities(zenodoMetadata, ctx.getCommunities().stream());
        addReferences(zenodoMetadata);

        return ZenodoMetaUtil.wrap(zenodoMetadata);
    }

    private static void addRecordedByIfAvailable(JsonNode multimedia, ObjectNode zenodoMetadata, String fieldName) throws MissingMetadataFieldException {
        if (hasFieldValue(multimedia, fieldName)) {
            addValueAsCustomFieldIfAvailable(zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_RECORDED_BY_ID, getValueOrThrow(multimedia, fieldName));
        }
    }

    private static boolean hasFieldValue(JsonNode multimedia, String fieldName) {
        return multimedia != null && multimedia.has(fieldName)
                && !multimedia.get(fieldName).isNull()
                && StringUtils.isNotBlank(multimedia.get(fieldName).asText());
    }

    private static void addReferences(ObjectNode zenodoMetadata) {
        ZenodoMetaUtil.append(zenodoMetadata, ZenodoMetaUtil.REFERENCES, "Hartop E, Srivathsan A, Ronquist F, Meier R (2022) Towards Large-Scale Integrative Taxonomy (LIT): resolving the data conundrum for dark taxa. Syst Biol 71:1404–1422. https://doi.org/10.1093/sysbio/syac033");
        ZenodoMetaUtil.append(zenodoMetadata, ZenodoMetaUtil.REFERENCES, "Srivathsan, A., Meier, R. (2024). Scalable, Cost-Effective, and Decentralized DNA Barcoding with Oxford Nanopore Sequencing. In: DeSalle, R. (eds) DNA Barcoding. Methods in Molecular Biology, vol 2744. Humana, New York, NY. https://doi.org/10.1007/978-1-0716-3581-0_14");
    }

    public static ObjectNode toPhysicalObjectDeposit(String jsonString, PublicationDateFactory publicationDateFactory, ZenodoConfig ctx) throws JsonProcessingException, MissingMetadataFieldException {
        JsonNode multimedia = new ObjectMapper().readTree(jsonString);

        ObjectNode zenodoMetadata = new ObjectMapper().createObjectNode();

        String eventId = getValueOrThrow(multimedia, DWC_TERMS_EVENT_ID);
        String occurrenceId = getValueOrThrow(multimedia, DWC_TERMS_OCCURRENCE_ID);
        String eventDate = getValueOrThrow(multimedia, DWC_TERMS_EVENT_DATE);

        addCustomFieldsIfAvailable(multimedia, zenodoMetadata);

        appendKeyImageIfDefinedOrMetadataJSONOtherwise(jsonString, multimedia, zenodoMetadata, "occurrence.json");

        String title = "Physical object " + occurrenceId + " sampled through event " + eventId + " on " + eventDate;
        zenodoMetadata.put(ZenodoMetaUtil.TITLE, title);

        setDescription(zenodoMetadata, title);


        ZenodoMetaUtil.appendIdentifier(zenodoMetadata, ZenodoMetaUtil.IS_ALTERNATE_IDENTIFIER, occurrenceId, PHYSICAL_OBJECT);
        ZenodoMetaUtil.appendIdentifier(zenodoMetadata, ZenodoMetaUtil.IS_VERSION_OF, ZenodoUtils.getSearchPageForExistingRecords(ctx, Arrays.asList(occurrenceId), PHYSICAL_OBJECT).getIRIString(), PHYSICAL_OBJECT);
        ZenodoMetaUtil.appendIdentifier(zenodoMetadata, ZenodoMetaUtil.IS_DERIVED_FROM, eventId, EVENT);
        ZenodoMetaUtil.appendIdentifier(zenodoMetadata, ZenodoMetaUtil.IS_DERIVED_FROM, ZenodoUtils.getSearchPageForExistingRecords(ctx, Arrays.asList(eventId), EVENT).getIRIString(), EVENT);
        ZenodoMetaUtil.appendIdentifier(zenodoMetadata, ZenodoMetaUtil.IS_DOCUMENTED_BY, ZenodoUtils.getSearchPageForExistingRecords(ctx, Arrays.asList(occurrenceId), RESOURCE_TYPE_PHOTO).getIRIString(), RESOURCE_TYPE_PHOTO);

        ZenodoMetaUtil.setValue(zenodoMetadata, ZenodoMetaUtil.UPLOAD_TYPE, ZenodoMetaUtil.UPLOAD_TYPE_PHYSICAL_OBJECT);
        ZenodoMetaUtil.setCreators(zenodoMetadata, Arrays.asList("Museum für Naturkunde Berlin"));
        ZenodoMetaUtil.setValue(zenodoMetadata, PUBLICATION_DATE, publicationDateFactory.getPublicationDate());
        ZenodoMetaUtil.setCommunities(zenodoMetadata, ctx.getCommunities().stream());
        addReferences(zenodoMetadata);

        return ZenodoMetaUtil.wrap(zenodoMetadata);
    }

    private static void appendKeyImageIfDefinedOrMetadataJSONOtherwise(String jsonString, JsonNode multimedia, ObjectNode zenodoMetadata, String metadataFilename) {
        String filename = null;
        String contentId = null;

        if (multimedia.has(DC_TERMS_DYNAMIC_PROPERTIES)) {
            String dynamicProperties = multimedia.get(DC_TERMS_DYNAMIC_PROPERTIES).asText();
            try {
                JsonNode properties = new ObjectMapper().readTree(dynamicProperties);
                JsonNode at = properties.at("/keyImageFilename");
                JsonNode id = properties.at("/keyImageId");
                if (!at.isMissingNode() && !id.isMissingNode()) {
                    String contentIdCandidate = id.asText();
                    if (HashKeyUtil.hashTypeFor(contentIdCandidate) != null) {
                        filename = at.asText();
                        contentId = HashKeyUtil.extractContentHash(RefNodeFactory.toIRI(contentIdCandidate)).getIRIString();
                    }
                }
            } catch (JsonProcessingException e) {
                LOG.warn("found dynamic properties, but failed to process [" + dynamicProperties + "]", e);
            }
        }
        if (StringUtils.isBlank(contentId)) {
            contentId = Hasher.calcHashIRI(jsonString, HashType.md5).getIRIString();
            filename = metadataFilename;
        }
        ZenodoMetaUtil.setFilename(zenodoMetadata, filename);
        appendAlternateIdentifiers(zenodoMetadata, contentId);
    }

    private static void addCustomFieldsIfAvailable(JsonNode multimedia, ObjectNode zenodoMetadata) throws MissingMetadataFieldException {
        addValueAsCustomFieldIfAvailable(zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_INSTITUTION_CODE, "MfN");

        appendEventDateAsZenodoCustomFieldIfAvailable(multimedia, zenodoMetadata);
        addFieldValueAsZenodoCustomFieldIfAvailable(multimedia, zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_VERBATIM_EVENT_DATE, DWC_TERMS_EVENT_DATE);
        addFieldValueAsZenodoCustomFieldIfAvailable(multimedia, zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_COUNTRY, DWC_TERMS_COUNTRY);
        addFieldValueAsZenodoCustomFieldIfAvailable(multimedia, zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_CATALOG_NUMBER, TERMS_CATALOG_NUMBER);
        addFieldValueAsZenodoCustomFieldIfAvailable(multimedia, zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_BASIS_OF_RECORD, DWC_TERMS_BASIS_OF_RECORD);
        addFieldValueAsZenodoCustomFieldIfAvailable(multimedia, zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_SCIENTIFIC_NAME, DWC_TERMS_SCIENTIFIC_NAME);
        addFieldValueAsZenodoCustomFieldIfAvailable(multimedia, zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_MATERIAL_SAMPLE_ID, DWC_TERMS_OCCURRENCE_ID);

        addFieldValueAsZenodoCustomFieldIfAvailable(multimedia, zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_LOCALITY, DWC_TERMS_LOCALITY);
        addFieldValueAsZenodoCustomFieldIfAvailable(multimedia, zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_RECORDED_BY, DWC_TERMS_RECORDED_BY);

        addRecordedByIfAvailable(multimedia, zenodoMetadata, DWC_TERMS_RECORDED_BY_ID);
        addRecordedByIfAvailable(multimedia, zenodoMetadata, GBIF_RECORDED_BY_ID);

        addFieldValueAsZenodoCustomFieldIfAvailable(multimedia, zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_AC_SUBJECT_PART, AC_TERMS_SUBJECT_PART);
        addFieldValueAsZenodoCustomFieldIfAvailable(multimedia, zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_AC_CAPTURE_DEVICE, AC_TERMS_CAPTURE_DEVICE);
        addFieldValueAsZenodoCustomFieldIfAvailable(multimedia, zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_AC_RESOURCE_CREATION_TECHNIQUE, AC_TERMS_RESOURCE_CREATION_TECHNIQUE);

    }

    private static void appendEventDateAsZenodoCustomFieldIfAvailable(JsonNode multimedia, ObjectNode zenodoMetadata) {
        if (multimedia.has(DWC_TERMS_EVENT_DATE)) {
            JsonNode valueNode = multimedia.get(DWC_TERMS_EVENT_DATE);
            if (!valueNode.isNull() && StringUtils.isNotBlank(valueNode.asText())) {
                String eventDate2 = valueNode.asText();
                String[] split = StringUtils.split(eventDate2, "/");
                if (split.length > 0) {
                    addValueAsCustomFieldIfAvailable(zenodoMetadata, ZenodoMetaUtil.FIELD_CUSTOM_DWC_EVENT_DATE, split[0]);
                }
            }
        }
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
