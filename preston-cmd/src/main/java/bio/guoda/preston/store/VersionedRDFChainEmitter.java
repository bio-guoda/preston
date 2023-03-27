package bio.guoda.preston.store;

import bio.guoda.preston.process.EmittingStreamFactory;
import bio.guoda.preston.process.EmittingStreamOfAnyVersions;
import bio.guoda.preston.process.ParsingEmitter;
import bio.guoda.preston.process.ProcessorReadOnly;
import bio.guoda.preston.process.ProcessorState;
import bio.guoda.preston.process.StatementEmitter;
import bio.guoda.preston.process.StatementsListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

import java.io.IOException;
import java.io.InputStream;

public class VersionedRDFChainEmitter extends ProcessorReadOnly {

    private static final Logger LOG = LoggerFactory.getLogger(VersionedRDFChainEmitter.class);
    private final EmittingStreamFactory emitterFactory = new EmittingStreamFactory() {
        @Override
        public ParsingEmitter createEmitter(StatementEmitter emitter, ProcessorState context) {
            return new EmittingStreamOfAnyVersions(emitter, context);
        }
    };

    public VersionedRDFChainEmitter(BlobStoreReadOnly blobStoreReadOnly, StatementsListener... listeners) {
        super(blobStoreReadOnly, listeners);
    }

    public VersionedRDFChainEmitter(BlobStoreReadOnly blobStoreReadOnly, ProcessorState state, StatementsListener... listeners) {
        super(blobStoreReadOnly, state, listeners);
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
