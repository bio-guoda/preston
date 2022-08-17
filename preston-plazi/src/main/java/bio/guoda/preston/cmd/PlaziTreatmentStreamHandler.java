package bio.guoda.preston.cmd;

import bio.guoda.preston.store.Dereferencer;
import bio.guoda.preston.stream.ContentStreamException;
import bio.guoda.preston.stream.ContentStreamHandler;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class PlaziTreatmentStreamHandler implements ContentStreamHandler {
    private static final Logger LOG = LoggerFactory.getLogger(PlaziTreatmentStreamHandler.class);

    private final Dereferencer<InputStream> dereferencer;
    private ContentStreamHandler contentStreamHandler;
    private final OutputStream outputStream;

    public PlaziTreatmentStreamHandler(ContentStreamHandler contentStreamHandler,
                                       Dereferencer<InputStream> inputStreamDereferencer,
                                       OutputStream os) {
        this.contentStreamHandler = contentStreamHandler;
        this.dereferencer = inputStreamDereferencer;
        this.outputStream = os;
    }

    @Override
    public boolean handle(IRI version, InputStream is) throws ContentStreamException {
        String iriString = version.getIRIString();
        if (!iriString.endsWith("/")) {
            try {
                handleAssumedPlaziTreatment(is, iriString, outputStream);
                return true;
            } catch (TreatmentParseException e) {
                // opportunistic parsing, skip those with parse exceptions
                return false;
            } catch (IOException ex) {
                String msg = "failed to handle [" + version.getIRIString() + "]";
                throw new ContentStreamException(msg, ex);
            }
        }
        return false;
    }

    protected static void handleAssumedPlaziTreatment(InputStream is,
                                                      String iriString,
                                                      OutputStream outputStream) throws IOException, TreatmentParseException {
        ObjectNode treatment = new ObjectMapper().createObjectNode();
        treatment.set("http://www.w3.org/ns/prov#wasDerivedFrom", TextNode.valueOf(iriString));
        treatment.set("http://www.w3.org/1999/02/22-rdf-syntax-ns#type", TextNode.valueOf("application/plazi+xml"));

        JsonNode parsedTreatment = new PlaziTreatmentParser().parse(is);

        Iterator<String> fieldNameIter = parsedTreatment.fieldNames();
        while (fieldNameIter.hasNext()) {
            String fieldName = fieldNameIter.next();
            treatment.put(fieldName, parsedTreatment.get(fieldName).asText());
        }

        if (treatment.size() > 2) {
            IOUtils.copy(IOUtils.toInputStream(treatment.toString(), StandardCharsets.UTF_8), outputStream);
            IOUtils.copy(IOUtils.toInputStream("\n", StandardCharsets.UTF_8), outputStream);
        }

    }


    @Override
    public boolean shouldKeepProcessing() {
        return contentStreamHandler.shouldKeepProcessing();
    }


}
