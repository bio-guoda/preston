package bio.guoda.preston.cmd;

import bio.guoda.preston.RefNodeConstants;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RISUtil {

    private static final Pattern RIS_KEY_VALUE = Pattern.compile("[^A-Z]*(?<key>[A-Z][A-Z2])[ ]+-(?<value>.*)");
    private static final Pattern BHL_PART_URL = Pattern.compile("(?<prefix>https://www.biodiversitylibrary.org/part/)(?<part>[0-9]+)");
    static final String TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";


    public static void parseRIS(InputStream inputStream, Consumer<ObjectNode> listener, String sourceIRIString) throws IOException {
        BufferedReader bufferedReader = IOUtils.toBufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));

        String line = null;
        ObjectNode record = null;
        long recordStart = -1;

        long lineNumber = 0;
        while ((line = bufferedReader.readLine()) != null) {
            lineNumber++;
            Matcher matcher = RIS_KEY_VALUE.matcher(line);
            if (matcher.matches()) {
                String key = matcher.group("key");
                String value = StringUtils.trim(matcher.group("value"));
                if ("TY".equals(key)) {
                    record = new ObjectMapper().createObjectNode();
                    record.put(key, value);
                    recordStart = lineNumber;
                } else if (record != null && recordStart > -1) {
                    if ("ER".equals(key)) {
                        record.put(RefNodeConstants.WAS_DERIVED_FROM.getIRIString(), "line:" + sourceIRIString + "!/L" + recordStart + "-L" + lineNumber);
                        record.put(TYPE, "application/x-research-info-systems");
                        listener.accept(record);
                        record = null;
                        recordStart = -1;
                    } else {
                        JsonNode jsonNode = record.get(key);
                        if (jsonNode == null) {
                            record.put(key, value);
                        } else {
                            ArrayNode array = null;
                            if (jsonNode.isArray()) {
                                array = (ArrayNode) jsonNode;
                                array.add(value);
                            } else {
                                array = new ObjectMapper().createArrayNode().add(jsonNode.asText()).add(value);
                            }
                            record.set(key, array);
                        }
                    }
                }
            }
        }
    }

    public static ObjectNode translateRISToZenodo(JsonNode jsonNode, List<String> communities) {
        ObjectNode metadata = new ObjectMapper().createObjectNode();
        ArrayNode relatedIdentifiers = new ObjectMapper().createArrayNode();
        metadata.put("description", "(Uploaded by Plazi from the Biodiversity Heritage Library) No abstract provided.");

        ZenodoMetaUtil.setCommunities(metadata, communities.stream());
        if (jsonNode.has(RefNodeConstants.WAS_DERIVED_FROM.getIRIString())) {
            String recordLocation = jsonNode.get(RefNodeConstants.WAS_DERIVED_FROM.getIRIString()).asText();
            String actionableLocation = StreamHandlerUtil.makeActionable(recordLocation);
            addDerivedFrom(relatedIdentifiers, actionableLocation);
            metadata.put(RefNodeConstants.WAS_DERIVED_FROM.getIRIString(), actionableLocation);
        }

        if (jsonNode.has(TYPE)) {
            metadata.put(TYPE, jsonNode.get(TYPE).asText());
        }
        if (jsonNode.has("TI")) {
            metadata.put("title", jsonNode.get("TI").asText());
        }
        if (jsonNode.has("T2")) {
            metadata.put("journal_title", jsonNode.get("T2").asText());
        }

        if (jsonNode.has(("VL"))) {
            metadata.put("journal_volume", jsonNode.get("VL").asText());
        }
        if (jsonNode.has("SP") && jsonNode.has("EP")) {
            metadata.put("journal_pages", jsonNode.get("SP").asText() + "-" + jsonNode.get("EP").asText());
        }
        if (jsonNode.has("PY")) {
            metadata.put("publication_date", jsonNode.get("PY").asText());

        }
        if (jsonNode.has("TY")) {
            if (StringUtils.equals(jsonNode.get("TY").asText(), "JOUR")) {
                metadata.put("publication_type", "article");
                metadata.put("upload_type", "publication");
            }

        }
        if (jsonNode.has("DO")) {
            String doiString = jsonNode.get("DO").asText();
            metadata.put("doi", doiString);
            addAlternateIdentifier(relatedIdentifiers, doiString);
        }

        if (jsonNode.has("UR")) {
            String url = jsonNode.get("UR").asText();
            metadata.put("referenceId", url);

            String downloadUrl = getBHLPartPDFUrl(metadata);
            if (StringUtils.isNotBlank(downloadUrl)) {
                addDerivedFrom(relatedIdentifiers, downloadUrl);
            }

            String filename = getBHLPartPdfFilename(metadata);
            if (StringUtils.isNotBlank(filename)) {
                metadata.put("filename", filename);
            }

            addDerivedFrom(relatedIdentifiers, url);
            String lsid = getBHLPartLSID(metadata);
            if (StringUtils.isNotBlank(lsid)) {
                addAlternateIdentifier(relatedIdentifiers, lsid);
            }
        }

        JsonNode authors = jsonNode.get("AU");
        if (authors != null) {
            ArrayNode creators = new ObjectMapper().createArrayNode();
            if (authors.isArray()) {
                authors.forEach(value -> creators.add(new ObjectMapper().createObjectNode().put("name", value.asText())));
            } else {
                creators.add(authors.asText());
            }
            metadata.set("creators", creators);
        }
        JsonNode keywords = jsonNode.get("KW");
        if (keywords != null) {
            ArrayNode creators = new ObjectMapper().createArrayNode();
            if (keywords.isArray()) {
                keywords.forEach(value -> creators.add(value.asText()));
            }
            metadata.set("keywords", creators);
        }

        metadata.set("related_identifiers", relatedIdentifiers);
        return metadata;
    }

    public static String getBHLPartPDFUrl(ObjectNode metadata) {
        Matcher matcher = getBHLPartIdMatcher(metadata);
        return matcher.matches()
                ? "https://www.biodiversitylibrary.org/partpdf/" + matcher.group("part")
                : null;
    }

    private static String getBHLPartLSID(ObjectNode metadata) {
        Matcher matcher = getBHLPartIdMatcher(metadata);
        return matcher.matches()
                ? "urn:lsid:biodiversitylibrary.org:part:" + matcher.group("part")
                : null;
    }

    private static String getBHLPartPdfFilename(ObjectNode metadata) {
        Matcher matcher = getBHLPartIdMatcher(metadata);
        return matcher.matches()
                ? "bhlpart" + matcher.group("part") + ".pdf"
                : null;
    }

    private static Matcher getBHLPartIdMatcher(ObjectNode metadata) {
        String url = "";
        if (metadata.has("referenceId")) {
            url = metadata.get("referenceId").asText();
        }

        return BHL_PART_URL.matcher(url);
    }


    private static ArrayNode addDerivedFrom(ArrayNode relatedIdentifiers, String s) {
        return addRelation(relatedIdentifiers, s, "isDerivedFrom");
    }

    private static ArrayNode addAlternateIdentifier(ArrayNode relatedIdentifiers, String s) {
        return addRelation(relatedIdentifiers, s, "isAlternateIdentifier");
    }

    private static ArrayNode addRelation(ArrayNode relatedIdentifiers, String s, String relationType) {
        return relatedIdentifiers.add(new ObjectMapper().createObjectNode().put("relation", relationType).put("identifier", s));
    }

}
