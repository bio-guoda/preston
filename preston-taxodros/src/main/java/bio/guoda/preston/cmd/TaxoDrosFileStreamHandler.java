package bio.guoda.preston.cmd;

import bio.guoda.preston.stream.ContentStreamException;
import bio.guoda.preston.stream.ContentStreamHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.txt.UniversalEncodingDetector;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class TaxoDrosFileStreamHandler implements ContentStreamHandler {

    public static final Map<String, String> TRANSLATION_MAP = new TreeMap<String, String>() {{
        put(".VN", "acceptedName");
        put(".FU", "originalSpecificEpithet");
        put(".OR", "originalGenus");
        put(".AU", "accordingTo");
        put(".FA", "family");
        put(".SF", "subfamily");
        put(".TR", "tribe");
        put(".ST", "subtribe");
        put(".IT", "infratribe");
        put(".GE", "genus");
        put(".SG", "subgenus");
        put(".GR", "speciesgroup");
        put(".SR", "speciessubgroup");
        put(".SC", "speciescomplex");
        put(".SS", "subspecies");
        put(".CO", "remarks");

    }};
    private static final String PREFIX_AUTHOR = ".A ";
    private static final String PREFIX_TITLE = ".S ";
    private static final String PREFIX_YEAR = ".J ";
    private static final String PREFIX_METHOD_DIGITIZATION = ".K ";
    private static final String PREFIX_JOURNAL = ".Z ";
    private static final String PREFIX_FILENAME = ".P ";
    public static final String LOCATIONS = "locations";
    public static final String TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    public static final String DROS_5 = "taxodros-dros5";
    public static final String DROS_3 = "taxodros-dros3";
    public static final String SYS = "taxodros-syst";
    private static final String PREFIX_PUBLISHER = ".Z.";
    public static final String REFERENCE_ID = "referenceId";
    public static final String JOURNAL_ISSUE = "journal_issue";
    public static final String JOURNAL_PAGES = "journal_pages";
    public static final String JOURNAL_VOLUME = "journal_volume";
    public static final String JOURNAL_TITLE = "journal_title";
    public static final String PUBLICATION_TYPE = "publication_type";
    public static final String IMPRINT_PUBLISHER = "imprint_publisher";
    public static final String PUBLICATION_DATE = "publication_date";

    public static final String TAXODROS_DATA_DOI = "10.5281/zenodo.10593902";
    public static final String TAXODROS_DATA_VERSION_SHA256 = "hash://sha256/e05466f33c755f11bd1c2fa30eef2388bf24ff7989931bae1426daff0200af19";
    public static final String TAXODROS_DATA_VERSION_MD5 = "hash://md5/4fa9eeed1c8cff2490483a48c718df02";

    private ContentStreamHandler contentStreamHandler;
    private final OutputStream outputStream;
    public static final String RELATED_IDENTIFIERS = "related_identifiers";

    public TaxoDrosFileStreamHandler(ContentStreamHandler contentStreamHandler,
                                     OutputStream os) {
        this.contentStreamHandler = contentStreamHandler;
        this.outputStream = os;
    }

    @Override
    public boolean handle(IRI version, InputStream is) throws ContentStreamException {
        AtomicBoolean foundAtLeastOne = new AtomicBoolean(false);
        String iriString = version.getIRIString();
        try {
            Charset charset = new UniversalEncodingDetector().detect(is, new Metadata());
            if (charset != null) {
                int lineStart = -1;
                int lineFinish = -1;
                AtomicReference<StringBuilder> textCapture = new AtomicReference<>(new StringBuilder());
                ObjectNode objectNode = new ObjectMapper().createObjectNode();

                BufferedReader reader = new BufferedReader(new InputStreamReader(is, charset));

                for (int lineNumber = 1; contentStreamHandler.shouldKeepProcessing(); ++lineNumber) {
                    String line = reader.readLine();
                    if (line == null || StringUtils.startsWith(line, ".TEXT;")) {
                        if (objectNode.size() > 0) {
                            if (isType(objectNode, DROS_5) || isType(objectNode, DROS_3)) {
                                if (isType(objectNode, DROS_5)) {
                                    setOriginReference(iriString, lineStart, lineFinish, objectNode);
                                    setValue(objectNode, "filename", getAndResetCapture(textCapture));
                                    setValue(objectNode, "upload_type", "publication");
                                    ArrayNode communitiesArray = Stream.of("taxodros", "biosyslit")
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
                                    setType(objectNode, DROS_5);
                                } else if (isType(objectNode, DROS_3)) {
                                    lineFinish = lineNumber - 1;
                                    setOriginReference(iriString, lineStart, lineFinish, objectNode);
                                }
                                writeRecord(foundAtLeastOne, objectNode);
                                lineFinish = -1;
                            }
                        }
                        lineStart = lineNumber;
                        objectNode.removeAll();
                        getAndResetCapture(textCapture);
                        if (line == null) {
                            break;
                        }
                    } else if (StringUtils.startsWith(line, PREFIX_AUTHOR)) {
                        setTypeDROS5(objectNode);
                        setValue(objectNode, REFERENCE_ID, getAndResetCapture(textCapture));
                        appendIdentifier(textCapture, line, PREFIX_AUTHOR);
                    } else if (StringUtils.startsWith(line, PREFIX_YEAR)) {
                        setTypeDROS5(objectNode);
                        String andResetCapture = getAndResetCapture(textCapture);
                        String[] authors = StringUtils.split(andResetCapture, ",");
                        ObjectNode creator = null;
                        ArrayNode creators = new ObjectMapper().createArrayNode();
                        StringBuilder creatorName = new StringBuilder();
                        for (int i = 0; i < authors.length; i++) {
                            String author = StringUtils.trim(StringUtils.replace(authors[i], "&", ""));
                            if (i % 2 == 0) {
                                creatorName.append(author);
                            } else {
                                creatorName.append(", ");
                                creatorName.append(author);
                                creator = new ObjectMapper().createObjectNode();
                                creator.put("name", creatorName.toString());
                                creators.add(creator);
                                creatorName = new StringBuilder();
                            }
                        }
                        objectNode.set("creators", creators);
                        appendIdentifier(textCapture, line, PREFIX_YEAR);
                    } else if (StringUtils.startsWith(line, PREFIX_TITLE)) {
                        setTypeDROS5(objectNode);
                        String publicationYear = getAndResetCapture(textCapture);
                        if (StringUtils.isNumeric(publicationYear)) {
                            if (publicationYear.startsWith("2") || publicationYear.length() < 4) {
                                setValue(objectNode, "access_right", "restricted");
                            }
                            setValue(objectNode, PUBLICATION_DATE, publicationYear);
                        }

                        appendIdentifier(textCapture, line, PREFIX_TITLE);
                    } else if (StringUtils.startsWith(line, PREFIX_PUBLISHER)) {
                        setTypeDROS5(objectNode);
                        if (objectNode.has(REFERENCE_ID)
                                && StringUtils.containsIgnoreCase(objectNode.get(REFERENCE_ID).asText(), "collection")) {
                            setValue(objectNode, PUBLICATION_TYPE, "other");
                            setValue(objectNode, "collection", objectNode.get(REFERENCE_ID).asText());
                        } else {
                            setValue(objectNode, PUBLICATION_TYPE, "book");
                        }
                        setValue(objectNode, "title", getAndResetCapture(textCapture));
                        appendIdentifier(textCapture, line, PREFIX_PUBLISHER);
                    } else if (StringUtils.startsWith(line, PREFIX_JOURNAL)) {
                        setTypeDROS5(objectNode);
                        setValue(objectNode, "title", getAndResetCapture(textCapture));
                        setValue(objectNode, PUBLICATION_TYPE, "article");
                        appendIdentifier(textCapture, line, PREFIX_JOURNAL);
                    } else if (StringUtils.startsWith(line, PREFIX_METHOD_DIGITIZATION)) {
                        setTypeDROS5(objectNode);
                        if (isArticle(objectNode)) {
                            String journalString = getAndResetCapture(textCapture);
                            String[] split = StringUtils.split(journalString, ",");
                            setValue(objectNode, JOURNAL_TITLE, split[0]);
                            if (split.length > 1) {
                                String remainder = StringUtils.substring(journalString, split[0].length() + 1);
                                enrichWithJournalInfo(objectNode, remainder);
                            }
                        } else {
                            setValue(objectNode, IMPRINT_PUBLISHER, getAndResetCapture(textCapture));
                        }
                        appendIdentifier(textCapture, line, PREFIX_METHOD_DIGITIZATION);
                    } else if (StringUtils.startsWith(line, PREFIX_FILENAME)) {
                        setTypeDROS5(objectNode);
                        String methodText = getAndResetCapture(textCapture);
                        Matcher matcher = Pattern.compile("(.*)(DOI|doi):(?<doi>[^ ]+)(.*)").matcher(methodText);
                        if (matcher.matches()) {
                            setValue(objectNode, "doi", matcher.group("doi"));
                        }
                        setValue(objectNode, "method", methodText);
                        appendIdentifier(textCapture, line, PREFIX_FILENAME);
                        lineFinish = lineNumber;
                    } else if (StringUtils.startsWith(line, ".DESC;")) {
                        setIdIfMissing(textCapture, objectNode);
                        setTypeDROS3(objectNode);
                    } else if (StringUtils.startsWith(line, "=e=")) {
                        setIdIfMissing(textCapture, objectNode);
                        String value = getValueWithLinePrefix(line, "=e=");
                        appendLocation(objectNode, LOCATIONS, value);
                    } else if (StringUtils.startsWith(line, ".KF=")) {
                        handleTaxonRecord(foundAtLeastOne, iriString, objectNode, lineNumber, line);
                    } else if (lineStart > lineFinish) {
                        if (isType(objectNode, DROS_3)) {
                            append(objectNode, "keywords", line);
                        } else {
                            appendIdentifier(textCapture, line);
                        }
                    }
                }
            }
        } catch (IOException e) {
            throw new ContentStreamException("no charset detected");
        }

        return foundAtLeastOne.get();
    }

    private void handleTaxonRecord(AtomicBoolean foundAtLeastOne, String iriString, ObjectNode objectNode, int lineNumber, String line) throws IOException {
        setOriginReference(iriString, lineNumber, lineNumber, objectNode);
        setType(objectNode, SYS);
        String[] row = StringUtils.split(line, '\t');
        for (String cellRaw : row) {
            String cell = StringUtils.trim(cellRaw);
            if (StringUtils.length(cell) > 3) {
                String key = StringUtils.substring(cell, 0, 3);
                String value = StringUtils.trim(StringUtils.substring(cell, 4, cell.length()));
                value = StringUtils.equals(value, ".") ? "" : value;
                setValue(objectNode, key, value);
                if (TRANSLATION_MAP.containsKey(key)) {
                    setValue(objectNode, TRANSLATION_MAP.get(key), value);
                }
                if (StringUtils.equals(key, ".KF")) {
                    setValue(objectNode, "taxonId", "urn:lsid:taxodros.uzh.ch:taxon:" + StringUtils.replace(value, " ", "_"));
                } else if (StringUtils.equals(key, ".AU")) {
                    setValue(objectNode, "referenceId", value);
                }
            }
        }
        writeRecord(foundAtLeastOne, objectNode);
    }

    private void writeRecord(AtomicBoolean foundAtLeastOne, ObjectNode objectNode) throws IOException {
        ObjectNode metadata = new ObjectMapper().createObjectNode();
        metadata.set("metadata", objectNode);
        IOUtils.copy(IOUtils.toInputStream(metadata.toString(), StandardCharsets.UTF_8), outputStream);
        IOUtils.copy(IOUtils.toInputStream("\n", StandardCharsets.UTF_8), outputStream);
        objectNode.removeAll();
        foundAtLeastOne.set(true);
    }

    private boolean isArticle(ObjectNode objectNode) {
        return objectNode.has(PUBLICATION_TYPE) && StringUtils.equals("article", objectNode.get(PUBLICATION_TYPE).textValue());
    }

    public static void enrichWithJournalInfo(ObjectNode objectNode, String remainder) {
        Pattern articleCoordinates = Pattern.compile("(?<volume>[^\\(:]+){0,1}(?<issue>\\([^)]*\\)){0,1}:(?<pages>[^.]*){0,1}([ .]*)$");
        Matcher matcher = articleCoordinates.matcher(remainder);
        if (matcher.matches()) {
            setValue(objectNode, JOURNAL_VOLUME, StringUtils.trim(matcher.group("volume")));
            setValue(objectNode, JOURNAL_PAGES, matcher.group("pages"));
            String issue = matcher.group("issue");
            if (StringUtils.isNotBlank(issue)) {
                setValue(objectNode, JOURNAL_ISSUE, StringUtils.substring(issue, 1, issue.length() - 1));
            }
        }
    }

    private void setTypeDROS3(ObjectNode objectNode) {
        setType(objectNode, DROS_3);
    }

    private void setTypeDROS5(ObjectNode objectNode) {
        setType(objectNode, "taxodros-dros5");
    }

    private void appendLocation(ObjectNode objectNode, String key, String value) {
        ArrayNode locations = objectNode.has(key)
                ? (ArrayNode) objectNode.get(key)
                : new ObjectMapper().createArrayNode();
        ObjectNode location = new ObjectMapper().createObjectNode();
        location.put("place", value);
        locations.add(location);
        objectNode.set(key, locations);
    }

    private void append(ObjectNode objectNode, String key, String value) {
        ArrayNode keywords = objectNode.has(key)
                ? (ArrayNode) objectNode.get(key)
                : new ObjectMapper().createArrayNode();
        keywords.add(value);
        objectNode.set(key, keywords);
    }

    private void setOriginReference(String iriString, int lineStart, int lineFinish, ObjectNode objectNode) {
        String suffix = lineFinish > lineStart
                ? "-" + "L" + lineFinish
                : "";
        String value = "line:" + iriString + "!/L" + lineStart + suffix;
        setValue(objectNode, "http://www.w3.org/ns/prov#wasDerivedFrom", value);
        appendIdentifier(objectNode, "isDerivedFrom", "https://linker.bio/" + value);
        appendIdentifier(objectNode, "isDerivedFrom", TAXODROS_DATA_DOI);
        appendIdentifier(objectNode, "isPartOf", "https://www.taxodros.uzh.ch");
        append(objectNode, "references", "Bächli, G. (2024). TaxoDros - The Database on Taxonomy of Drosophilidae " + TAXODROS_DATA_VERSION_MD5 + " " + TAXODROS_DATA_VERSION_SHA256 + " [Data set]. Zenodo. " + "https://doi.org/" + TAXODROS_DATA_DOI);
    }

    private void appendIdentifier(ObjectNode objectNode, String relationType, String value) {
        ArrayNode relatedIdentifiers = objectNode.has(TaxoDrosFileStreamHandler.RELATED_IDENTIFIERS) && objectNode.get(TaxoDrosFileStreamHandler.RELATED_IDENTIFIERS).isArray()
                ? (ArrayNode) objectNode.get(TaxoDrosFileStreamHandler.RELATED_IDENTIFIERS)
                : new ObjectMapper().createArrayNode();
        relatedIdentifiers.add(new ObjectMapper().createObjectNode()
                .put("relation", relationType)
                .put("identifier", value)
        );
        objectNode.set(TaxoDrosFileStreamHandler.RELATED_IDENTIFIERS, relatedIdentifiers);
    }

    private void setType(ObjectNode objectNode, String type) {
        setValue(objectNode, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", type);
    }

    private boolean isType(ObjectNode objectNode, String typeValue) {
        return objectNode.has(TYPE)
                && StringUtils.equals(typeValue, objectNode.get(TYPE).asText());
    }

    private void setIdIfMissing(AtomicReference<StringBuilder> textCapture, ObjectNode objectNode) {
        if (!objectNode.has(REFERENCE_ID)) {
            setValue(objectNode, REFERENCE_ID, getAndResetCapture(textCapture));
        }
    }

    private void appendIdentifier(AtomicReference<StringBuilder> textCapture, String line, String prefix) {
        String value = getValueWithLinePrefix(line, prefix);
        if (StringUtils.isNotBlank(value)) {
            appendIdentifier(textCapture, value);
        }
    }

    private StringBuilder appendIdentifier(AtomicReference<StringBuilder> textCapture, String line) {
        return textCapture.get().append(" ").append(StringUtils.trim(line));
    }

    private String getAndResetCapture(AtomicReference<StringBuilder> captureBuffer) {
        StringBuilder captured = captureBuffer.getAndSet(new StringBuilder());
        return StringUtils.trim(captured.toString());
    }


    private static void setValue(ObjectNode objectNode, String key, String value) {
        if (value != null) {
            objectNode.set(key, TextNode.valueOf(value));
        }
    }

    private static String getValueWithLinePrefix(String line, CharSequence prefix) {
        int start = prefix.length();
        int end = line.length();
        return StringUtils.trim(
                StringUtils.substring(line, start, end));
    }

    @Override
    public boolean shouldKeepProcessing() {
        return contentStreamHandler.shouldKeepProcessing();
    }


}
