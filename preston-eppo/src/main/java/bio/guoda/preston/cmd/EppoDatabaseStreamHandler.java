package bio.guoda.preston.cmd;

import bio.guoda.preston.store.Dereferencer;
import bio.guoda.preston.stream.ContentStreamException;
import bio.guoda.preston.stream.ContentStreamHandler;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.regex.Pattern;

public class EppoDatabaseStreamHandler extends ContentStreamHandler {
    private static final Logger LOG = LoggerFactory.getLogger(EppoDatabaseStreamHandler.class);

    private final Dereferencer<InputStream> dereferencer;
    private ContentStreamHandler contentStreamHandler;
    private final OutputStream outputStream;

    public EppoDatabaseStreamHandler(ContentStreamHandler contentStreamHandler,
                                     Dereferencer<InputStream> inputStreamDereferencer,
                                     OutputStream os) {
        this.contentStreamHandler = contentStreamHandler;
        this.dereferencer = inputStreamDereferencer;
        this.outputStream = os;
    }

    @Override
    public boolean handle(IRI version, InputStream is) throws ContentStreamException {
        throw new ContentStreamException("please provide a source for [" + version.getIRIString() + "]");
    }

    @Override
    public boolean handle(IRI version, InputStream is, IRI source) throws ContentStreamException {
        String iriString = version.getIRIString();
        if (source != null && StringUtils.contains(source.getIRIString(), "eppo.int")) {
            try {
                handleAssumedEppoDataRecord(is, iriString, outputStream, source);
                return true;
            } catch (IOException ex) {
                throw new ContentStreamException("failed to handle [" + version.getIRIString() + "]", ex);
            }
        }
        return false;
    }

    protected static void handleAssumedEppoDataRecord(InputStream is,
                                                      String iriString,
                                                      OutputStream outputStream,
                                                      IRI source) throws IOException {

        JsonNode parsedRecord = new ObjectMapper().readTree(is);

        if (parsedRecord.isArray()) {
            for (JsonNode singleRecord : parsedRecord) {
                annotateAndCopy(iriString, outputStream, singleRecord, source);
            }
        } else {
            annotateAndCopy(iriString, outputStream, parsedRecord, source);
        }

    }

    private static void annotateAndCopy(String iriString,
                                        OutputStream outputStream,
                                        JsonNode singleRecord,
                                        IRI source) throws IOException {
        ObjectNode annotatedRecord = annotateRecord(iriString, singleRecord, source);
        copyRecordTo(annotatedRecord, outputStream);
    }

    private static void copyRecordTo(ObjectNode jsonNodes, OutputStream outputStream) throws IOException {
        IOUtils.copy(IOUtils.toInputStream(jsonNodes.toString(), StandardCharsets.UTF_8), outputStream);
        IOUtils.copy(IOUtils.toInputStream("\n", StandardCharsets.UTF_8), outputStream);
    }

    private static ObjectNode annotateRecord(String iriString, JsonNode singleRecord, IRI source) {
        ObjectNode record = new ObjectMapper().createObjectNode();
        record.set("http://www.w3.org/ns/prov#wasDerivedFrom", TextNode.valueOf(iriString));
        record.set("http://www.w3.org/ns/prov#wasRevisionOf", TextNode.valueOf(source.getIRIString()));
        record.set("http://www.w3.org/1999/02/22-rdf-syntax-ns#type", TextNode.valueOf("application/json"));
        Iterator<String> fieldNameIter = singleRecord.fieldNames();
        while (fieldNameIter.hasNext()) {
            String fieldName = fieldNameIter.next();
            record.put(fieldName, singleRecord.get(fieldName).asText());
        }
        return record;
    }


    @Override
    public boolean shouldKeepProcessing() {
        return contentStreamHandler.shouldKeepProcessing();
    }


}
