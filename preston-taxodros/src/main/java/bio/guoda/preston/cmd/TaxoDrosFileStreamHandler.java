package bio.guoda.preston.cmd;

import bio.guoda.preston.stream.ContentStreamException;
import bio.guoda.preston.stream.ContentStreamHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
    public static final String DROS_5 = "taxodros-dros5";
    public static final String DROS_3 = "taxodros-dros3";
    public static final String SYS = "taxodros-syst";
    private static final String PREFIX_PUBLISHER = ".Z.";
    public static final String PROP_TAXODROS_DATA_DOI = "taxodros.data.doi";
    public static final String PROP_TAXODROS_DATA_VERSION_SHA256 = "taxodros.data.version.sha256";
    public static final String PROP_TAXODROS_DATA_VERSION_MD5 = "taxodros.data.version.md5";
    public static final String PROP_TAXODROS_DATA_YEAR = "taxodros.data.year";

    private final Properties props;

    public static final String TAXODROS_DATA_DOI = "10.5281/zenodo.13841002";
    public static final String TAXODROS_DATA_VERSION_SHA256 = "hash://sha256/a6e757007c04215cafa537c09a06a0e8a68be70cbc9c965c857c7a9058ceeb16";
    public static final String TAXODROS_DATA_VERSION_MD5 = "hash://md5/b3ead19ea211a66e4f59a6842e097c7b";
    public static final String TAXODROS_DATA_YEAR = "2024";
    public static final String LSID_PREFIX = "urn:lsid:taxodros.uzh.ch";

    private ContentStreamHandler contentStreamHandler;
    private final OutputStream outputStream;
    private List<String> communities;

    public TaxoDrosFileStreamHandler(ContentStreamHandler contentStreamHandler,
                                     OutputStream os,
                                     List<String> communities,
                                     Properties props) {
        this.contentStreamHandler = contentStreamHandler;
        this.outputStream = os;
        this.communities = communities;
        this.props = props;
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
                            lineFinish = closeRecordAndWrite(foundAtLeastOne, iriString, lineStart, lineFinish, textCapture, objectNode, lineNumber);
                        }
                        lineStart = lineNumber;
                        objectNode.removeAll();
                        getAndResetCapture(textCapture);
                        if (line == null) {
                            break;
                        }
                    } else if (StringUtils.startsWith(line, PREFIX_AUTHOR)) {
                        setTypeDROS5(objectNode);
                        setKeywords(objectNode);
                        String referenceId = getAndResetCapture(textCapture);
                        ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.REFERENCE_ID, referenceId);
                        ZenodoMetaUtil.appendIdentifier(objectNode, ZenodoMetaUtil.IS_ALTERNATE_IDENTIFIER, LSID_PREFIX + ":id:" + JavaScriptAndPythonFriendlyURLEncodingUtil.urlEncode(referenceId));
                        appendIdentifier(textCapture, line, PREFIX_AUTHOR);
                    } else if (StringUtils.startsWith(line, PREFIX_YEAR)) {
                        setTypeDROS5(objectNode);
                        String authorStrings = getAndResetCapture(textCapture);

                        List<String> creatorList = collectCreators(authorStrings);

                        ZenodoMetaUtil.setCreators(objectNode, creatorList);
                        appendIdentifier(textCapture, line, PREFIX_YEAR);

                    } else if (StringUtils.startsWith(line, PREFIX_TITLE)) {
                        setTypeDROS5(objectNode);
                        String publicationYear = getAndResetCapture(textCapture);
                        TreeMap<String, String> pubYearTranslations = new TreeMap<String, String>() {{
                            put("202o", "2020");
                            put("985", "1985");
                            put("191", "1961");
                        }};
                        String pubYearTranslated = pubYearTranslations.getOrDefault(publicationYear, publicationYear);
                        try {
                            ZenodoMetaUtil.setPublicationDate(objectNode, pubYearTranslated);
                        } catch (IllegalArgumentException ex) {
                            closeRecord(iriString, lineStart, lineNumber - 1, textCapture, objectNode, lineNumber);
                            throw new ContentStreamException("suspicious publication year [" + publicationYear + "] found in [" + objectNode.toPrettyString() + "]", ex);
                        }
                        appendIdentifier(textCapture, line, PREFIX_TITLE);
                    } else if (StringUtils.startsWith(line, PREFIX_PUBLISHER)) {
                        setTypeDROS5(objectNode);
                        String title = getAndResetCapture(textCapture);
                        ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.TITLE, title);
                        if (objectNode.has(ZenodoMetaUtil.REFERENCE_ID)
                                && StringUtils.containsIgnoreCase(objectNode.get(ZenodoMetaUtil.REFERENCE_ID).asText(), "collection")) {
                            ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.PUBLICATION_TYPE, ZenodoMetaUtil.PUBLICATION_TYPE_OTHER);
                            ZenodoMetaUtil.setValue(objectNode, "collection", objectNode.get(ZenodoMetaUtil.REFERENCE_ID).asText());
                        } else {
                            Pattern pageNumberPattern = Pattern.compile("(?<title>.*)[, ]+(?<pagesSignature>[ ][p]{1,2}\\.)[ ]+(?<pages>[0-9 -]+).*");
                            Matcher matcher = pageNumberPattern.matcher(title);
                            if (matcher.matches()) {
                                ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.PARTOF_PAGES, matcher.group("pages"));
                                ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.PARTOF_TITLE, matcher.group("title"));
                                ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.PUBLICATION_TYPE, ZenodoMetaUtil.PUBLICATION_TYPE_BOOK_SECTION);
                            } else {
                                ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.PARTOF_TITLE, title);
                                ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.PUBLICATION_TYPE, ZenodoMetaUtil.PUBLICATION_TYPE_BOOK);
                            }
                        }
                        appendIdentifier(textCapture, line, PREFIX_PUBLISHER);
                    } else if (StringUtils.startsWith(line, PREFIX_JOURNAL)) {
                        setTypeDROS5(objectNode);
                        ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.TITLE, getAndResetCapture(textCapture));
                        ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.PUBLICATION_TYPE, ZenodoMetaUtil.PUBLICATION_TYPE_ARTICLE);
                        appendIdentifier(textCapture, line, PREFIX_JOURNAL);
                    } else if (StringUtils.startsWith(line, PREFIX_METHOD_DIGITIZATION)) {
                        setTypeDROS5(objectNode);
                        if (isArticle(objectNode)) {
                            String journalString = getAndResetCapture(textCapture);
                            int commaIndex = StringUtils.lastIndexOf(journalString, ',');
                            String journalTitle = journalString;
                            String remainder = "";
                            if (commaIndex > 0) {
                                journalTitle = StringUtils.substring(journalString, 0, commaIndex);
                                remainder = StringUtils.substring(journalString, commaIndex + 1);
                            }
                            setJournalTitle(objectNode, journalTitle);
                            enrichWithJournalInfo(objectNode, remainder);
                        } else {
                            ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.IMPRINT_PUBLISHER, getAndResetCapture(textCapture));
                        }
                        appendIdentifier(textCapture, line, PREFIX_METHOD_DIGITIZATION);
                    } else if (StringUtils.startsWith(line, PREFIX_FILENAME)) {
                        setTypeDROS5(objectNode);
                        String methodText = getAndResetCapture(textCapture);
                        Matcher matcher = Pattern.compile("(.*)(10[.])(?<doiPrefix>[0-9]+)/(?<doiSuffix>[^ ]+)(.*)").matcher(methodText);
                        if (matcher.matches()) {
                            String doiString = "10." + matcher.group("doiPrefix") + "/" + matcher.group("doiSuffix");
                            ZenodoMetaUtil.appendIdentifier(objectNode, ZenodoMetaUtil.IS_ALTERNATE_IDENTIFIER, doiString);
                        }
                        ZenodoMetaUtil.setValue(objectNode, "taxodros:method", methodText);
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
                            ZenodoMetaUtil.append(objectNode, "keywords", line);
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

    private int closeRecordAndWrite(AtomicBoolean foundAtLeastOne, String iriString, int lineStart, int lineFinish, AtomicReference<StringBuilder> textCapture, ObjectNode objectNode, int lineNumber) throws IOException {
        lineFinish = closeRecord(iriString, lineStart, lineFinish, textCapture, objectNode, lineNumber);
        if (isDROS(objectNode)) {
            writeRecord(foundAtLeastOne, objectNode);
            lineFinish = -1;
        }
        return lineFinish;
    }

    private int closeRecord(String iriString, int lineStart, int lineFinish, AtomicReference<StringBuilder> textCapture, ObjectNode objectNode, int lineNumber) {
        if (isDROS(objectNode)) {
            if (isType(objectNode, DROS_5)) {
                setOriginReference(iriString, lineStart, lineFinish, objectNode);
                String filename = getAndResetCapture(textCapture);
                ZenodoMetaUtil.setFilename(objectNode, filename);
                ZenodoMetaUtil.appendIdentifier(objectNode, ZenodoMetaUtil.IS_ALTERNATE_IDENTIFIER, LSID_PREFIX + ":filename:" + JavaScriptAndPythonFriendlyURLEncodingUtil.urlEncode(filename));
                ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.UPLOAD_TYPE, ZenodoMetaUtil.UPLOAD_TYPE_PUBLICATION);
                ZenodoMetaUtil.setCommunities(objectNode, communities.stream());
                ZenodoMetaUtil.setType(objectNode, DROS_5);
            } else if (isType(objectNode, DROS_3)) {
                lineFinish = lineNumber - 1;
                setOriginReference(iriString, lineStart, lineFinish, objectNode);
            }
        }
        return lineFinish;
    }

    private boolean isDROS(ObjectNode objectNode) {
        return isType(objectNode, DROS_5) || isType(objectNode, DROS_3);
    }

    private List<String> collectCreators(String authorStrings) {
        String[] authorsFirstSplit = StringUtils.split(authorStrings, "&");
        List<String> creatorList = new ArrayList<>();

        for (String s : authorsFirstSplit) {
            String[] authors = StringUtils.split(s, ",");
            StringBuilder creatorName = new StringBuilder();
            for (int i = 0; i < authors.length; i++) {
                String author = StringUtils.trim(StringUtils.replace(authors[i], "&", ""));
                if (i % 2 == 0) {
                    creatorName.append(author);
                } else {
                    creatorName.append(", ");
                    creatorName.append(author);
                    creatorList.add(creatorName.toString());
                    creatorName = new StringBuilder();
                }
            }
        }
        return creatorList;
    }

    private void setJournalTitle(ObjectNode objectNode, String value) {
        ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.JOURNAL_TITLE, value);
    }

    private void handleTaxonRecord(AtomicBoolean foundAtLeastOne, String iriString, ObjectNode objectNode, int lineNumber, String line) throws IOException {
        setOriginReference(iriString, lineNumber, lineNumber, objectNode);
        ZenodoMetaUtil.setType(objectNode, SYS);
        String[] row = StringUtils.split(line, '\t');
        for (String cellRaw : row) {
            String cell = StringUtils.trim(cellRaw);
            if (StringUtils.length(cell) > 3) {
                String key = StringUtils.substring(cell, 0, 3);
                String value = StringUtils.trim(StringUtils.substring(cell, 4, cell.length()));
                value = StringUtils.equals(value, ".") ? "" : value;
                ZenodoMetaUtil.setValue(objectNode, key, value);
                if (TRANSLATION_MAP.containsKey(key)) {
                    ZenodoMetaUtil.setValue(objectNode, TRANSLATION_MAP.get(key), value);
                }
                if (StringUtils.equals(key, ".KF")) {
                    ZenodoMetaUtil.setValue(objectNode, "taxonId", LSID_PREFIX + ":taxon:" + StringUtils.replace(value, " ", "_"));
                } else if (StringUtils.equals(key, ".AU")) {
                    ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.REFERENCE_ID, value);
                }
            }
        }
        writeRecord(foundAtLeastOne, objectNode);
    }

    private void writeRecord(AtomicBoolean foundAtLeastOne, ObjectNode objectNode) throws IOException {
        objectNode.put("description", "Uploaded by Plazi for TaxoDros. We do not have abstracts.");
        ObjectNode metadata = new ObjectMapper().createObjectNode();
        metadata.set("metadata", objectNode);
        IOUtils.copy(IOUtils.toInputStream(metadata.toString(), StandardCharsets.UTF_8), outputStream);
        IOUtils.copy(IOUtils.toInputStream("\n", StandardCharsets.UTF_8), outputStream);
        objectNode.removeAll();
        foundAtLeastOne.set(true);
    }

    private boolean isArticle(ObjectNode objectNode) {
        return objectNode.has(ZenodoMetaUtil.PUBLICATION_TYPE) && StringUtils.equals("article", objectNode.get(ZenodoMetaUtil.PUBLICATION_TYPE).textValue());
    }

    public static void enrichWithJournalInfo(ObjectNode objectNode, String remainder) {
        Pattern articleCoordinates = Pattern.compile("(?<volume>[^\\(:]+){0,1}(?<issue>\\([^)]*\\)){0,1}:(?<pages>[^.]*){0,1}([ .]*)$");
        Matcher matcher = articleCoordinates.matcher(remainder);
        if (matcher.matches()) {
            ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.JOURNAL_VOLUME, StringUtils.trim(matcher.group("volume")));
            ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.JOURNAL_PAGES, matcher.group("pages"));
            String issue = matcher.group("issue");
            if (StringUtils.isNotBlank(issue)) {
                ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.JOURNAL_ISSUE, StringUtils.substring(issue, 1, issue.length() - 1));
            }
        }
    }

    private void setTypeDROS3(ObjectNode objectNode) {
        ZenodoMetaUtil.setType(objectNode, DROS_3);
    }

    private void setTypeDROS5(ObjectNode objectNode) {
        ZenodoMetaUtil.setType(objectNode, "taxodros-dros5");
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

    private void setOriginReference(String iriString, int lineStart, int lineFinish, ObjectNode objectNode) {
        String suffix = lineFinish > lineStart
                ? "-" + "L" + lineFinish
                : "";

        String doi = props.getProperty(PROP_TAXODROS_DATA_DOI);
        String md5 = props.getProperty(PROP_TAXODROS_DATA_VERSION_MD5);
        String sha256 = props.getProperty(PROP_TAXODROS_DATA_VERSION_SHA256);

        String value = "line:" + iriString + "!/L" + lineStart + suffix;
        ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.WAS_DERIVED_FROM, value);
        ZenodoMetaUtil.appendIdentifier(objectNode, ZenodoMetaUtil.IS_DERIVED_FROM, "https://linker.bio/" + value);
        ZenodoMetaUtil.appendIdentifier(objectNode, ZenodoMetaUtil.IS_DERIVED_FROM, doi);
        ZenodoMetaUtil.appendIdentifier(objectNode, ZenodoMetaUtil.IS_PART_OF, "https://www.taxodros.uzh.ch");
        ZenodoMetaUtil.appendIdentifier(objectNode, ZenodoMetaUtil.IS_PART_OF, md5);
        ZenodoMetaUtil.appendIdentifier(objectNode, ZenodoMetaUtil.IS_PART_OF, sha256);
        ZenodoMetaUtil.append(objectNode, ZenodoMetaUtil.REFERENCES, "Bächli, G. (" + props.getProperty(PROP_TAXODROS_DATA_YEAR) + "). TaxoDros - The Database on Taxonomy of Drosophilidae " + md5 + " " + sha256 + " [Data set]. Zenodo. " + "https://doi.org/" + doi);

    }

    private void setKeywords(ObjectNode objectNode) {
        ZenodoMetaUtil.addKeyword(objectNode, "Biodiversity");
        ZenodoMetaUtil.addKeyword(objectNode, "Taxonomy");
        ZenodoMetaUtil.addKeyword(objectNode, "fruit flies");
        ZenodoMetaUtil.addKeyword(objectNode, "flies");
        ZenodoMetaUtil.addKeyword(objectNode, "Animalia");
        ZenodoMetaUtil.addKeyword(objectNode, "Arthropoda");
        ZenodoMetaUtil.addKeyword(objectNode, "Insecta");
        ZenodoMetaUtil.addKeyword(objectNode, "Diptera");

        ZenodoMetaUtil.addCustomField(objectNode, ZenodoMetaUtil.FIELD_CUSTOM_DWC_KINGDOM, "Animalia");
        ZenodoMetaUtil.addCustomField(objectNode, ZenodoMetaUtil.FIELD_CUSTOM_DWC_PHYLUM, "Arthropoda");
        ZenodoMetaUtil.addCustomField(objectNode, ZenodoMetaUtil.FIELD_CUSTOM_DWC_CLASS, "Insecta");
        ZenodoMetaUtil.addCustomField(objectNode, ZenodoMetaUtil.FIELD_CUSTOM_DWC_ORDER, "Diptera");
    }

    private boolean isType(ObjectNode objectNode, String typeValue) {
        return objectNode.has(ZenodoMetaUtil.TYPE)
                && StringUtils.equals(typeValue, objectNode.get(ZenodoMetaUtil.TYPE).asText());
    }

    private void setIdIfMissing(AtomicReference<StringBuilder> textCapture, ObjectNode objectNode) {
        if (!objectNode.has(ZenodoMetaUtil.REFERENCE_ID)) {
            ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.REFERENCE_ID, getAndResetCapture(textCapture));
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
