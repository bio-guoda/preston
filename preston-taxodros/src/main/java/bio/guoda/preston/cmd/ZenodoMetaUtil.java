package bio.guoda.preston.cmd;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.stream.Stream;

public class ZenodoMetaUtil {
    public static final String IS_DERIVED_FROM = "isDerivedFrom";
    public static final String IS_PART_OF = "isPartOf";
    public static final String REFERENCES = "references";
    public static final String RELATED_IDENTIFIERS = "related_identifiers";
    public static final String WAS_DERIVED_FROM = "http://www.w3.org/ns/prov#wasDerivedFrom";
    public static final String TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    public static final String REFERENCE_ID = "referenceId";
    public static final String JOURNAL_ISSUE = "journal_issue";
    public static final String JOURNAL_PAGES = "journal_pages";
    public static final String JOURNAL_VOLUME = "journal_volume";
    public static final String JOURNAL_TITLE = "journal_title";
    public static final String PUBLICATION_TYPE = "publication_type";
    public static final String PUBLICATION_DATE = "publication_date";
    public static final String PUBLICATION_TYPE_ARTICLE = "article";
    public static final String TITLE = "title";

    public static final String FIELD_CUSTOM_DWC_KINGDOM = "dwc:kingdom";
    public static final String FIELD_CUSTOM_DWC_PHYLUM = "dwc:phylum";
    public static final String FIELD_CUSTOM_DWC_CLASS = "dwc:class";
    public static final String FIELD_CUSTOM_DWC_ORDER = "dwc:order";
    public static final String DOI = "doi";
    static final String KEYWORDS = "keywords";
    static final String CUSTOM = "custom";

    public static void setCommunities(ObjectNode objectNode, Stream<String> communities) {
        ArrayNode communitiesArray = communities
                .map(id -> {
                    ObjectNode objectNode1 = new ObjectMapper().createObjectNode();
                    objectNode1.put("identifier", id);
                    return objectNode1;
                })
                .reduce(new ObjectMapper().createArrayNode(),
                        ArrayNode::add,
                        ArrayNode::add
                );
        objectNode.set("communities", communitiesArray);
    }

    public static void append(ObjectNode objectNode, String key, String value) {
        ArrayNode keywords = objectNode.has(key)
                ? (ArrayNode) objectNode.get(key)
                : new ObjectMapper().createArrayNode();
        keywords.add(value);
        objectNode.set(key, keywords);
    }

    public static void setValue(ObjectNode objectNode, String key, String value) {
        if (value != null) {
            objectNode.set(key, TextNode.valueOf(value));
        }
    }

    public static void appendIdentifier(ObjectNode objectNode, String relationType, String value) {
        ArrayNode relatedIdentifiers = objectNode.has(RELATED_IDENTIFIERS) && objectNode.get(RELATED_IDENTIFIERS).isArray()
                ? (ArrayNode) objectNode.get(RELATED_IDENTIFIERS)
                : new ObjectMapper().createArrayNode();
        relatedIdentifiers.add(new ObjectMapper().createObjectNode()
                .put("relation", relationType)
                .put("identifier", value)
        );
        objectNode.set(RELATED_IDENTIFIERS, relatedIdentifiers);
    }

    public static void setCreators(ObjectNode objectNode, List<String> creatorList) {
        ArrayNode creators = new ObjectMapper().createArrayNode();
        for (String creatorName : creatorList) {
            ObjectNode creator = new ObjectMapper().createObjectNode();
            creator.put("name", creatorName);
            creators.add(creator);
        }
        objectNode.set("creators", creators);
    }

    public static void setPublicationDate(ObjectNode objectNode, String publicationYear) {
        if (StringUtils.isNumeric(publicationYear)) {
            if (publicationYear.startsWith("2") || publicationYear.length() < 4) {
                setValue(objectNode, "access_right", "restricted");
            }
            setValue(objectNode, PUBLICATION_DATE, publicationYear);
        }
    }

    public static void addKeyword(ObjectNode objectNode, String biodiversity) {
        ArrayNode relatedIdentifiers = objectNode.has(KEYWORDS) && objectNode.get(KEYWORDS).isArray()
                ? (ArrayNode) objectNode.get(KEYWORDS)
                : new ObjectMapper().createArrayNode();
        relatedIdentifiers.add(biodiversity);
        objectNode.set(KEYWORDS, relatedIdentifiers);
    }

    public static void addCustomField(ObjectNode objectNode, String fieldName, String fieldValue) {
        ObjectNode relatedIdentifiers = objectNode.has(CUSTOM) && objectNode.get(CUSTOM).isObject()
                ? (ObjectNode) objectNode.get(CUSTOM)
                : new ObjectMapper().createObjectNode();
        relatedIdentifiers.set(fieldName, new ObjectMapper().createArrayNode().add(fieldValue));
        objectNode.set(CUSTOM, relatedIdentifiers);
    }
}
