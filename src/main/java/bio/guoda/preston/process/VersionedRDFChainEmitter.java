package bio.guoda.preston.process;

import bio.guoda.preston.store.VersionUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Triple;
import org.apache.jena.shared.JenaException;

import java.io.IOException;
import java.io.InputStream;

public class VersionedRDFChainEmitter extends ProcessorReadOnly {

    private static final Log LOG = LogFactory.getLog(VersionedRDFChainEmitter.class);

    public VersionedRDFChainEmitter(BlobStoreReadOnly blobStoreReadOnly, StatementListener... listeners) {
        super(blobStoreReadOnly, listeners);
    }

    @Override
    public void on(Triple statement) {
        IRI version = VersionUtil.mostRecentVersionForStatement(statement);
        emitProvenanceLogVersion(version);
    }

    public void emitProvenanceLogVersion(IRI version) {
        if (version != null) {
            try {
                InputStream inputStream = get(version);
                if (inputStream != null) {
                    parseAndEmit(inputStream, this);
                }
            } catch (IOException e) {
                LOG.warn("failed to read archive [" + version + "]", e);
            } catch (JenaException ex) {
                throw new RuntimeException("failed to read archive [" + version + "]", ex);
            }
        }
    }

    public void parseAndEmit(InputStream inputStream) {
        parseAndEmit(inputStream, this);
    }

    public static void parseAndEmit(InputStream inputStream, final StatementEmitter emitter) {
        new EmittingStreamRDF(emitter)
                .parseAndEmit(inputStream);
    }


}
