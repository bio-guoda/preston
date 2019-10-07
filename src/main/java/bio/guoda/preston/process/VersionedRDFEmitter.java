package bio.guoda.preston.process;

import bio.guoda.preston.store.VersionUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Triple;
import org.apache.jena.shared.JenaException;

import java.io.IOException;
import java.io.InputStream;

public class VersionedRDFEmitter extends ProcessorReadOnly {

    private static final Log LOG = LogFactory.getLog(VersionedRDFEmitter.class);

    public VersionedRDFEmitter(BlobStoreReadOnly blobStoreReadOnly, StatementListener... listeners) {
        super(blobStoreReadOnly, listeners);
    }

    @Override
    public void on(Triple statement) {
        IRI version = VersionUtil.mostRecentVersionForStatement(statement);

        if (version != null) {
            try {
                InputStream inputStream = get(version);
                if (inputStream != null) {
                    parseAndEmit(inputStream);
                }
            } catch (IOException e) {
                LOG.warn("failed to read archive [" + version + "]", e);
            } catch (JenaException ex) {
                throw new RuntimeException("failed to read archive [" + version + "]", ex);
            }
        }
    }

    void parseAndEmit(InputStream inputStream) {
        new EmittingStreamRDF(this)
                .parseAndEmit(inputStream);
    }


}
