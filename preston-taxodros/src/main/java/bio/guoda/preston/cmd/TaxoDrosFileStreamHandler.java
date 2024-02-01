package bio.guoda.preston.cmd;

import bio.guoda.preston.store.Dereferencer;
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
import org.eclipse.rdf4j.common.text.StringUtil;

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

    }};
    private static final String PREFIX_AUTHOR = ".A ";
    private static final String PREFIX_TITLE = ".S ";
    private static final String PREFIX_YEAR = ".J ";
    private static final String PREFIX_METHOD_DIGITIZATION = ".K ";
    private static final String PREFIX_JOURNAL = ".Z ";
    private static final String PREFIX_FILENAME = ".P ";
    public static final String LOCALITIES = "localities";
    public static final String TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    public static final String DROS_5 = "taxodros-dros5";
    public static final String DROS_3 = "taxodros-dros3";
    public static final String SYS = "taxodros-syst";
    private static final String PREFIX_PUBLISHER = ".Z.";
    public static final String REFERENCE_ID = "referenceId";

    private ContentStreamHandler contentStreamHandler;
    private final OutputStream outputStream;

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
                                    setType(objectNode, DROS_5);
                                } else if (isType(objectNode, DROS_3)) {
                                    lineFinish = lineNumber - 1;
                                    setOriginReference(iriString, lineStart, lineFinish, objectNode);
                                    setValue(objectNode, "keywords", getAndResetCapture(textCapture));
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
                        append(textCapture, line, PREFIX_AUTHOR);
                    } else if (StringUtils.startsWith(line, PREFIX_YEAR)) {
                        setTypeDROS5(objectNode);
                        setValue(objectNode, "authors", getAndResetCapture(textCapture));
                        append(textCapture, line, PREFIX_YEAR);
                    } else if (StringUtils.startsWith(line, PREFIX_TITLE)) {
                        setTypeDROS5(objectNode);
                        setValue(objectNode, "year", getAndResetCapture(textCapture));
                        append(textCapture, line, PREFIX_TITLE);
                    } else if (StringUtils.startsWith(line, PREFIX_PUBLISHER)) {
                        setTypeDROS5(objectNode);
                        if (objectNode.has(REFERENCE_ID)
                                && StringUtils.containsIgnoreCase(objectNode.get(REFERENCE_ID).asText(), "collection")) {
                            setValue(objectNode, "type", "collection");
                            setValue(objectNode, "collection", objectNode.get(REFERENCE_ID).asText());
                        } else {
                            setValue(objectNode, "type", "book");
                        }
                        setValue(objectNode, "title", getAndResetCapture(textCapture));
                        append(textCapture, line, PREFIX_PUBLISHER);
                    } else if (StringUtils.startsWith(line, PREFIX_JOURNAL)) {
                        setTypeDROS5(objectNode);
                        setValue(objectNode, "title", getAndResetCapture(textCapture));
                        setValue(objectNode, "type", "article");
                        append(textCapture, line, PREFIX_JOURNAL);
                    } else if (StringUtils.startsWith(line, PREFIX_METHOD_DIGITIZATION)) {
                        setTypeDROS5(objectNode);
                        if (isArticle(objectNode)) {
                            String journalString = getAndResetCapture(textCapture);
                            String[] split = StringUtils.split(journalString, ",");
                            setValue(objectNode, "journal", split[0]);
                            if (split.length > 1) {
                                String remainder = StringUtils.substring(journalString, split[0].length() + 1);
                                enrichWithJournalInfo(objectNode, remainder);
                            }
                        } else {
                            setValue(objectNode, "publisher", getAndResetCapture(textCapture));
                        }
                        append(textCapture, line, PREFIX_METHOD_DIGITIZATION);
                    } else if (StringUtils.startsWith(line, PREFIX_FILENAME)) {
                        setTypeDROS5(objectNode);
                        String methodText = getAndResetCapture(textCapture);
                        Matcher matcher = Pattern.compile("(.*)(DOI|doi):(?<doi>[^ ]+)(.*)").matcher(methodText);
                        if (matcher.matches()) {
                            setValue(objectNode, "doi", matcher.group("doi"));
                        }
                        setValue(objectNode, "method", methodText);
                        append(textCapture, line, PREFIX_FILENAME);
                        lineFinish = lineNumber;
                    } else if (StringUtils.startsWith(line, ".DESC;")) {
                        setIdIfMissing(textCapture, objectNode);
                        setTypeDROS3(objectNode);
                    } else if (StringUtils.startsWith(line, "=e=")) {
                        setIdIfMissing(textCapture, objectNode);
                        String value = getValueWithLinePrefix(line, "=e=");
                        append(objectNode, LOCALITIES, value);
                    } else if (StringUtils.startsWith(line, ".KF=")) {
                        handleTaxonRecord(foundAtLeastOne, iriString, objectNode, lineNumber, line);
                    }
                      else if (lineStart > lineFinish) {
                        if (isType(objectNode, DROS_3)) {
                            append(objectNode, "keywords", line);
                        } else {
                            append(textCapture, line);
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
        setOriginReference(iriString, lineNumber, lineNumber, objectNode);
        writeRecord(foundAtLeastOne, objectNode);
    }

    private void writeRecord(AtomicBoolean foundAtLeastOne, ObjectNode objectNode) throws IOException {
        IOUtils.copy(IOUtils.toInputStream(objectNode.toString(), StandardCharsets.UTF_8), outputStream);
        IOUtils.copy(IOUtils.toInputStream("\n", StandardCharsets.UTF_8), outputStream);
        objectNode.removeAll();
        foundAtLeastOne.set(true);
    }

    private boolean isArticle(ObjectNode objectNode) {
        return objectNode.has("type") && StringUtils.equals("article", objectNode.get("type").textValue());
    }

    public static void enrichWithJournalInfo(ObjectNode objectNode, String remainder) {
        Pattern articleCoordinates = Pattern.compile("(?<volume>[^\\(:]+){0,1}(?<number>\\([^)]*\\)){0,1}:(?<pages>[^.]*){0,1}([ .]*)$");
        Matcher matcher = articleCoordinates.matcher(remainder);
        if (matcher.matches()) {
            setValue(objectNode, "volume", StringUtils.trim(matcher.group("volume")));
            setValue(objectNode, "pages", matcher.group("pages"));
            String number = matcher.group("number");
            if (StringUtils.isNotBlank(number)) {
                setValue(objectNode, "number", StringUtils.substring(number, 1, number.length() - 1));
            }
        }
    }

    private void setTypeDROS3(ObjectNode objectNode) {
        setType(objectNode, DROS_3);
    }

    private void setTypeDROS5(ObjectNode objectNode) {
        setType(objectNode, "taxodros-dros5");
    }

    private void append(ObjectNode objectNode, String key, String value) {
        ArrayNode localities = objectNode.has(key)
                ? (ArrayNode) objectNode.get(key)
                : new ObjectMapper().createArrayNode();
        localities.add(value);
        objectNode.set(key, localities);
    }

    private void setOriginReference(String iriString, int lineStart, int lineFinish, ObjectNode objectNode) {
        String suffix = lineFinish > lineStart
                ? "-" + "L" + lineFinish
                : "";
        setValue(objectNode, "http://www.w3.org/ns/prov#wasDerivedFrom", "line:" + iriString + "!/L" + lineStart + suffix);
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

    private void append(AtomicReference<StringBuilder> textCapture, String line, String prefix) {
        String value = getValueWithLinePrefix(line, prefix);
        if (StringUtils.isNotBlank(value)) {
            append(textCapture, value);
        }
    }

    private StringBuilder append(AtomicReference<StringBuilder> textCapture, String line) {
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
