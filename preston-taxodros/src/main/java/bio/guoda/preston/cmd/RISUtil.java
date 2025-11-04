package bio.guoda.preston.cmd;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.process.ProcessorState;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.ByteOrderMark;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RISUtil {

    public static final Pattern DATE_RANGE_PATTERN = Pattern.compile("(?<start>[0-9]{4})-(?<end>[0-9]{4})");
    public static final Pattern RIS_TAG_PATTERN = Pattern.compile("^[A-Z0-9]+$");
    public static final String RIS_JOURNAL_ISSUE = "IS";
    private static final Pattern RIS_KEY_VALUE = Pattern.compile("[^A-Z]*(?<key>[A-Z][A-Z2])[ ]+-(?<value>.*)");
    private static final Pattern BHL_PART_URL = Pattern.compile("(?<prefix>https://www.biodiversitylibrary.org/part/)(?<part>[0-9]+)");
    static final String TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    public static final String RIS_TITLE = "TI";
    public static final String RIS_PUBLICATION_TYPE = "TY";
    public static final String RIS_JOURNAL_TITLE = "T2";
    public static final String RIS_JOURNAL_VOLUME = "VL";
    public static final String RIS_JOURNAL_PAGES = "SP";
    public static final String JOURNAL_ISSUE = "IS";
    public static final String RIS_PUBLICATION_DATE = "PY";
    public static final String RIS_IMPRINT_PUBLISHER = "PB";
    public static final String RIS_DOI = "DO";
    public static final String RIS_SERIAL_NUMBER = "SN";
    public static final String RIS_URL = "UR";
    public static final String RIS_AUTHOR_NAME = "AU";
    public static final String RIS_KEYWORD = "KW";
    public static final String NO_ABSTRACT_PROVIDED = "No abstract provided.";
    public static final String RIS_ABSTRACT = "AB";
    public static final String RIS_END_OF_RECORD = "ER";


    public static void parseRIS(InputStream inputStream, Consumer<ObjectNode> listener, String sourceIRIString, ProcessorState state) throws IOException {
        BufferedReader bufferedReader = IOUtils.toBufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));

        String line = null;
        ObjectNode record = null;
        long recordStart = -1;

        long lineNumber = 0;
        while ((line = bufferedReader.readLine()) != null && state.shouldKeepProcessing()) {
            lineNumber++;
            Matcher matcher = RIS_KEY_VALUE.matcher(line);
            if (matcher.matches()) {
                String key = matcher.group("key");
                String value = trim(matcher.group("value"));
                if (RIS_PUBLICATION_TYPE.equals(key)) {
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

    public static ObjectNode translateRISToZenodo(JsonNode jsonNode, List<String> communities, boolean reuseProvidedDOIAsZenodoDOI) {
        ObjectNode metadata = new ObjectMapper().createObjectNode();
        ArrayNode relatedIdentifiers = new ObjectMapper().createArrayNode();
        String abstractText = jsonNode.has(RIS_ABSTRACT)
                ? jsonNode.get(RIS_ABSTRACT).asText()
                : NO_ABSTRACT_PROVIDED;
        metadata.put("description", "(Uploaded by Plazi from the Biodiversity Heritage Library)" + " " + abstractText);

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

        if (jsonNode.has(RIS_TITLE)) {
            metadata.put("title", jsonNode.get(RIS_TITLE).asText());
        }

        if (jsonNode.has(RIS_PUBLICATION_TYPE)) {
            String value = jsonNode.get(RIS_PUBLICATION_TYPE).asText();
            if (StringUtils.equals("BOOK", value) || StringUtils.equals("CPAPER", value)) {
                metadata.put(ZenodoMetaUtil.UPLOAD_TYPE, "publication");
                if (jsonNode.has(RIS_IMPRINT_PUBLISHER) && StringUtils.isNotBlank(jsonNode.get(RIS_IMPRINT_PUBLISHER).asText())) {
                    metadata.put(ZenodoMetaUtil.PUBLICATION_TYPE, ZenodoMetaUtil.PUBLICATION_TYPE_BOOK);
                } else {
                    metadata.put(ZenodoMetaUtil.PUBLICATION_TYPE, ZenodoMetaUtil.PUBLICATION_TYPE_OTHER);
                }

            } else if (StringUtils.equals("CHAP", value)) {
                metadata.put(ZenodoMetaUtil.UPLOAD_TYPE, "publication");
                metadata.put(ZenodoMetaUtil.PUBLICATION_TYPE, ZenodoMetaUtil.PUBLICATION_TYPE_BOOK_SECTION);
            } else if (StringUtils.equals("JOUR", value)) {
                metadata.put(ZenodoMetaUtil.UPLOAD_TYPE, "publication");
                metadata.put(ZenodoMetaUtil.PUBLICATION_TYPE, ZenodoMetaUtil.PUBLICATION_TYPE_ARTICLE);
            }

        }
        if (jsonNode.has(RIS_JOURNAL_TITLE)) {
            metadata.put("journal_title", jsonNode.get(RIS_JOURNAL_TITLE).asText());
        }


        if (jsonNode.has(RIS_JOURNAL_VOLUME)) {
            metadata.put("journal_volume", jsonNode.get(RIS_JOURNAL_VOLUME).asText());
        }
        if (jsonNode.has(RIS_JOURNAL_PAGES) && jsonNode.has("EP")) {
            metadata.put("journal_pages", jsonNode.get(RIS_JOURNAL_PAGES).asText() + "-" + jsonNode.get("EP").asText());
        }
        if (jsonNode.has(JOURNAL_ISSUE)) {
            metadata.put("journal_issue", jsonNode.get(JOURNAL_ISSUE).asText());
        }
        if (jsonNode.has(RIS_PUBLICATION_DATE)) {
            String publicationYear = jsonNode.get(RIS_PUBLICATION_DATE).asText();
            Matcher matcher = DATE_RANGE_PATTERN.matcher(publicationYear);
            if (matcher.matches()) {
                publicationYear = matcher.group("start") + "/" + matcher.group("end");
            }
            metadata.put("publication_date", publicationYear);
        }
        if (jsonNode.has(RIS_IMPRINT_PUBLISHER)) {
            metadata.put("imprint_publisher", jsonNode.get(RIS_IMPRINT_PUBLISHER).asText());
        }
        if (jsonNode.has(RIS_PUBLICATION_TYPE)) {
            if (StringUtils.equals(jsonNode.get(RIS_PUBLICATION_TYPE).asText(), "JOUR")) {
                metadata.put("publication_type", "article");
                metadata.put("upload_type", "publication");
            }

        }
        if (jsonNode.has(RIS_DOI)) {
            String doiString = jsonNode.get(RIS_DOI).asText();
            addAlternateIdentifier(relatedIdentifiers, doiString);
            if (reuseProvidedDOIAsZenodoDOI) {
                metadata.put("doi", doiString);
            }
        }

        if (jsonNode.has(RIS_SERIAL_NUMBER)) {
            String sn = jsonNode.get(RIS_SERIAL_NUMBER).asText();
            Matcher matcher = Pattern.compile("^[0-9]{4}-[0-9]{3}[0-9X]$").matcher(sn);
            if (matcher.matches()) {
                addAlternateIdentifier(relatedIdentifiers, sn, "issn");
            } else {
                Matcher isbn = Pattern.compile("^([0-9]{10}|[0-9]{13})$").matcher(sn);
                if (isbn.matches()) {
                    addAlternateIdentifier(relatedIdentifiers, sn, "isbn");
                }
            }
        }

        if (jsonNode.has(RIS_URL)) {
            String url = jsonNode.get(RIS_URL).asText();
            metadata.put("referenceId", url);

            String filename = getBHLPartPdfFilename(metadata);
            if (StringUtils.isNotBlank(filename)) {
                metadata.put("filename", filename);
            }

            addDerivedFrom(relatedIdentifiers, url);
            String lsid = getBHLPartLSID(metadata);
            if (StringUtils.isNotBlank(lsid)) {
                addAlternateIdentifier(relatedIdentifiers, lsid);
            }

            if (StringUtils.contains(url, "biodiversitylibrary.org")) {
                addKeyword(metadata, "Biodiversity");
                addKeyword(metadata, "BHL-Corpus");
                addKeyword(metadata, "Source: Biodiversity Heritage Library");
                addKeyword(metadata, "Source: https://biodiversitylibrary.org");
                addKeyword(metadata, "Source: BHL");
            }
        }

        JsonNode authors = jsonNode.get(RIS_AUTHOR_NAME);
        if (authors != null) {
            ArrayNode creators = new ObjectMapper().createArrayNode();
            if (authors.isArray()) {
                authors.forEach(value -> creators.add(new ObjectMapper().createObjectNode().put("name", removeTrailingComma(value.asText()))));
            } else {
                creators.add(new ObjectMapper().createObjectNode().put("name", removeTrailingComma(authors.asText())));
            }
            metadata.set("creators", creators);
        }
        JsonNode keywords = jsonNode.get(RIS_KEYWORD);
        if (keywords != null) {
            final ArrayNode keywordsList = getKeywordList(metadata);
            if (keywords.isArray()) {
                keywords.forEach(value -> keywordsList.add(value.asText()));
            } else {
                keywordsList.add(keywords.asText());
            }
            metadata.set("keywords", keywordsList);
        }

        metadata.set("related_identifiers", relatedIdentifiers);

        // see https://github.com/bio-guoda/preston/issues/299
        // Zenodo rejects records without creators (aka authors)
        addDefaultAuthorIfNoneAvailable(metadata);

        return metadata;
    }

    private static ArrayNode getKeywordList(ObjectNode metadata) {
        final ArrayNode keywordList = new ObjectMapper().createArrayNode();
        JsonNode keywordsExisting = metadata.at("/keywords");
        if (keywordsExisting.isArray()) {
            keywordList.addAll((ArrayNode) keywordsExisting);
        }
        return keywordList;
    }

    public static void addKeyword(ObjectNode metadata, String keyword) {
        ArrayNode keywordList = getKeywordList(metadata);
        keywordList.add(keyword);
        metadata.set("keywords", keywordList);
    }

    public static String removeTrailingComma(String author) {
        return RegExUtils.removePattern(author, ",$");
    }

    private static void addDefaultAuthorIfNoneAvailable(ObjectNode metadata) {
        if (metadata.at("/creators/0/name").isMissingNode()) {
            metadata.set("creators", new ObjectMapper().createArrayNode().add(new ObjectMapper().createObjectNode().put("name", "NA")));
        }
    }

    private static String trim(String str) {
        String s = StringUtils.replaceChars(str, ByteOrderMark.UTF_BOM, ' ');
        return StringUtils.trim(s);
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


    private static ArrayNode addDerivedFrom(ArrayNode relatedIdentifiers, String id) {
        return addRelation(relatedIdentifiers, id, ZenodoMetaUtil.IS_DERIVED_FROM);
    }

    private static ArrayNode addAlternateIdentifier(ArrayNode relatedIdentifiers, String id) {
        return addRelation(relatedIdentifiers, id, ZenodoMetaUtil.IS_ALTERNATE_IDENTIFIER);
    }

    private static ArrayNode addAlternateIdentifier(ArrayNode relatedIdentifiers, String id, String scheme) {
        return addRelation(relatedIdentifiers, id, ZenodoMetaUtil.IS_ALTERNATE_IDENTIFIER, scheme);
    }

    private static ArrayNode addRelation(ArrayNode relatedIdentifiers, String id, String relationType) {
        return relatedIdentifiers.add(
                createRelationNode(id, relationType)
        );
    }

    private static ObjectNode createRelationNode(String id, String relationType) {
        return new ObjectMapper()
                .createObjectNode()
                .put("relation", relationType)
                .put("identifier", id);
    }

    private static ArrayNode addRelation(ArrayNode relatedIdentifiers, String id, String relationType, String scheme) {
        return relatedIdentifiers.add(
                createRelationNode(id, relationType)
                        .put("scheme", scheme)
        );
    }

    public static void writeAsRIS(ObjectNode objectNode, OutputStream os) throws IOException {
        if (objectNode.has(RIS_PUBLICATION_TYPE)) {
            OutputStreamWriter printWriter = new OutputStreamWriter(os, StandardCharsets.UTF_8);
            writeObject(printWriter, RIS_PUBLICATION_TYPE, objectNode.get(RIS_PUBLICATION_TYPE));
            Iterator<String> fieldNames = objectNode.fieldNames();
            while (fieldNames.hasNext()) {
                String fieldName = fieldNames.next();
                if (!org.apache.commons.codec.binary.StringUtils.equals(RIS_PUBLICATION_TYPE, fieldName)) {
                    if (RIS_TAG_PATTERN.matcher(fieldName).matches()) {
                        JsonNode jsonNode = objectNode.get(fieldName);
                        if (jsonNode.isArray()) {
                            for (JsonNode valueNode : jsonNode) {
                                writeObject(printWriter, fieldName, valueNode);
                            }
                        } else {
                            writeObject(printWriter, fieldName, jsonNode);
                        }
                    }
                }
            }
            printWriter.write(RIS_END_OF_RECORD + "  - \r\n");
            printWriter.flush();
        }
    }

    private static void writeObject(OutputStreamWriter printWriter, String fieldName, JsonNode jsonNode) throws IOException {
        printWriter.write(fieldName + "  - " + jsonNode.asText() + "\r\n");
    }
}
