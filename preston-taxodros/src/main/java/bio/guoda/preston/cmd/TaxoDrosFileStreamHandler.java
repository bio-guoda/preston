package bio.guoda.preston.cmd;

import bio.guoda.preston.store.Dereferencer;
import bio.guoda.preston.stream.ContentStreamException;
import bio.guoda.preston.stream.ContentStreamHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TaxoDrosFileStreamHandler implements ContentStreamHandler {

    private static final String PREFIX_AUTHOR = ".A ";
    private static final String PREFIX_TITLE = ".S ";
    private static final String PREFIX_YEAR = ".J ";
    private static final String PREFIX_METHOD_DIGITIZATION = ".K ";
    private static final String PREFIX_JOURNAL = ".Z ";
    private static final String PREFIX_FILENAME = ".P ";

    private final Dereferencer<InputStream> dereferencer;
    private ContentStreamHandler contentStreamHandler;
    private final OutputStream outputStream;

    public TaxoDrosFileStreamHandler(ContentStreamHandler contentStreamHandler,
                                     Dereferencer<InputStream> inputStreamDereferencer,
                                     OutputStream os) {
        this.contentStreamHandler = contentStreamHandler;
        this.dereferencer = inputStreamDereferencer;
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
                        if (lineFinish > lineStart
                                && objectNode.size() > 0) {
                            if (isLikelyDROS5Record(objectNode)) {
                                setValue(objectNode, "filename", getAndResetCapture(textCapture));
                                setValue(objectNode, "http://www.w3.org/ns/prov#wasDerivedFrom", "line:" + iriString + "!/L" + lineStart + "-" + "L" + lineFinish);
                                setValue(objectNode, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "taxodros-flatfile");
                                IOUtils.copy(IOUtils.toInputStream(objectNode.toString(), StandardCharsets.UTF_8), outputStream);
                                IOUtils.copy(IOUtils.toInputStream("\n", StandardCharsets.UTF_8), outputStream);
                                foundAtLeastOne.set(true);
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
                        setValue(objectNode, "id", getAndResetCapture(textCapture));
                        append(textCapture, line, PREFIX_AUTHOR);
                    } else if (StringUtils.startsWith(line, PREFIX_YEAR)) {
                        setValue(objectNode, "authors", getAndResetCapture(textCapture));
                        append(textCapture, line, PREFIX_YEAR);
                    } else if (StringUtils.startsWith(line, PREFIX_TITLE)) {
                        setValue(objectNode, "year", getAndResetCapture(textCapture));
                        append(textCapture, line, PREFIX_TITLE);
                    } else if (StringUtils.startsWith(line, PREFIX_JOURNAL)) {
                        setValue(objectNode, "title", getAndResetCapture(textCapture));
                        append(textCapture, line, PREFIX_JOURNAL);
                    } else if (StringUtils.startsWith(line, PREFIX_METHOD_DIGITIZATION)) {
                        setValue(objectNode, "journal", getAndResetCapture(textCapture));
                        append(textCapture, line, PREFIX_METHOD_DIGITIZATION);
                    } else if (StringUtils.startsWith(line, PREFIX_FILENAME)) {
                        String methodText = getAndResetCapture(textCapture);
                        Matcher matcher = Pattern.compile("(.*)(DOI|doi):(?<doi>[^ ]+)(.*)").matcher(methodText);
                        if (matcher.matches()) {
                            setValue(objectNode, "doi", matcher.group("doi"));
                        }
                        setValue(objectNode, "method", methodText);
                        append(textCapture, line, PREFIX_FILENAME);
                        lineFinish = lineNumber;
                    } else if (StringUtils.startsWith(line, ".DESC;")) {
                        // DROS3 file not support yet
                        lineFinish = lineNumber;
                    } else if (lineStart > lineFinish){
                        append(textCapture, line);
                    }
                }
            }
        } catch (IOException e) {
            throw new ContentStreamException("no charset detected");
        }

        return foundAtLeastOne.get();
    }

    private boolean isLikelyDROS5Record(ObjectNode objectNode) {
        return objectNode.has("authors")
                && objectNode.has("title")
                && objectNode.has("id")
                && objectNode.has("journal")
                && objectNode.has("year");
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


    private void setValue(ObjectNode objectNode, String key, String value) {
        if (StringUtils.isNotBlank(value)) {
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
