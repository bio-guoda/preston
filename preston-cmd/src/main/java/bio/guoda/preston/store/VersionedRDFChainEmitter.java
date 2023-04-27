package bio.guoda.preston.store;

import bio.guoda.preston.process.EmittingStreamFactory;
import bio.guoda.preston.process.ProcessorReadOnly;
import bio.guoda.preston.process.ProcessorState;
import bio.guoda.preston.process.StatementsListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

import java.io.IOException;
import java.io.InputStream;

public class VersionedRDFChainEmitter extends ProcessorReadOnly {

    private static final Logger LOG = LoggerFactory.getLogger(VersionedRDFChainEmitter.class);
    private final EmittingStreamFactory emitterFactory;

    public VersionedRDFChainEmitter(BlobStoreReadOnly blobStoreReadOnly,
                                    EmittingStreamFactory emitterFactory,
                                    StatementsListener... listeners) {
        super(blobStoreReadOnly, listeners);
        this.emitterFactory = emitterFactory;
    }

    public VersionedRDFChainEmitter(BlobStoreReadOnly blobStoreReadOnly,
                                    ProcessorState state,
                                    EmittingStreamFactory emitterFactory,
                                    StatementsListener... listeners) {
        super(blobStoreReadOnly, state, listeners);
        this.emitterFactory = emitterFactory;
    }

    @Override
    public void on(Quad statement) {
        IRI version = VersionUtil.mostRecentVersion(statement);
        emitProvenanceLogVersion(version);
    }

    private void emitProvenanceLogVersion(IRI version) {
        if (version != null) {
            try {
                InputStream inputStream = get(version);
                if (inputStream != null) {
                    parseAndEmit(inputStream);
                }
            } catch (IOException e) {
                LOG.warn("failed to read archive [" + version + "]", e);
            }
        }
    }

    public void parseAndEmit(InputStream inputStream) {
        emitterFactory
                .createEmitter(VersionedRDFChainEmitter.this, getState())
                .parseAndEmit(inputStream);
    }


}
