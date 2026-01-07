package bio.guoda.preston.cmd;

import bio.guoda.preston.stream.ContentStreamException;
import bio.guoda.preston.stream.ContentStreamHandler;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.rdf.api.IRI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class ZoteroFileStreamHandlerAbstract implements ContentStreamHandler {

    private final Logger LOG = LoggerFactory.getLogger(ZoteroFileStreamHandlerAbstract.class);


    private final IRI provenanceAnchor;
    private final ContentStreamHandler contentStreamHandler;
    private final OutputStream outputStream;

    public ZoteroFileStreamHandlerAbstract(ContentStreamHandler contentStreamHandler,
                                           OutputStream os,
                                           IRI provenanceAnchor) {
        this.contentStreamHandler = contentStreamHandler;
        this.outputStream = os;
        this.provenanceAnchor = provenanceAnchor;
    }

    @Override
    public boolean handle(IRI version, InputStream is) throws ContentStreamException {
        AtomicBoolean foundAtLeastOne = new AtomicBoolean(false);
        String iriString = version.getIRIString();
        try {
            JsonNode zoteroRecord = new ObjectMapper().readTree(is);
            if (zoteroRecord.isObject()) {
                handleZoteroRecord(zoteroRecord, iriString, foundAtLeastOne);
            }
        } catch (IOException e) {
            // opportunistic parsing, so ignore exceptions
        } catch (IllegalArgumentException ex) {
            LOG.warn("possible marformed Zotero records in [" + version + "]", ex);
        }
        return foundAtLeastOne.get();
    }

    abstract void handleZoteroRecord(JsonNode zoteroRecord, String iriString, AtomicBoolean foundAtLeastOne) throws ContentStreamException, IOException;


    @Override
    public boolean shouldKeepProcessing() {
        return contentStreamHandler.shouldKeepProcessing();
    }


    public IRI getProvenanceAnchor() {
        return provenanceAnchor;
    }

    public OutputStream getOutputStream() {
        return outputStream;
    }
}
