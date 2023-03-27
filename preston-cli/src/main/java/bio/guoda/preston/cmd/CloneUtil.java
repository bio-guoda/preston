package bio.guoda.preston.cmd;

import bio.guoda.preston.HashType;
import bio.guoda.preston.process.EmittingStreamFactory;
import bio.guoda.preston.process.EmittingStreamOfAnyVersions;
import bio.guoda.preston.process.ParsingEmitter;
import bio.guoda.preston.process.ProcessorState;
import bio.guoda.preston.process.StatementEmitter;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.process.StatementsListenerAdapter;
import bio.guoda.preston.store.BlobStoreAppendOnly;

import bio.guoda.preston.store.HashKeyUtil;
import bio.guoda.preston.store.KeyValueStore;
import bio.guoda.preston.store.ProvenanceTracer;
import bio.guoda.preston.store.VersionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

import java.io.IOException;
import java.io.InputStream;

import static bio.guoda.preston.cmd.ReplayUtil.attemptReplay;

public class CloneUtil {
    private static final Logger LOG = LoggerFactory.getLogger(CloneUtil.class);

    public static void clone(KeyValueStore blobKeyValueStore,
                             KeyValueStore provenanceLogKeyValueStore,
                             HashType type,
                             ProvenanceTracer provenanceTracer,
                             IRI provenanceRoot) {

        final BlobStoreReadOnly blobStore
                = new BlobStoreAppendOnly(blobKeyValueStore, true, type);

        final BlobStoreReadOnly provenanceLogStore
                = new BlobStoreAppendOnly(provenanceLogKeyValueStore, true, type);

        StatementsListener statementListener = blobToucher(blobStore);
        attemptReplay(provenanceLogStore, provenanceRoot, provenanceTracer, new EmittingStreamFactory() {
            @Override
            public ParsingEmitter createEmitter(StatementEmitter emitter, ProcessorState context) {
                return new EmittingStreamOfAnyVersions(emitter, context);
            }
        }, statementListener);
    }

    private static StatementsListener blobToucher(final BlobStoreReadOnly blobStore) {
        // touch blob, to let keyValueStore know about it
        return new StatementsListenerAdapter() {
            @Override
            public void on(Quad statement) {
                IRI mostRecent = VersionUtil.mostRecentVersion(statement);
                if (mostRecent != null && HashKeyUtil.isValidHashKey(mostRecent)) {
                    try (InputStream is = blobStore.get(mostRecent)) {
                    } catch (IOException e) {
                        LOG.warn("failed to copy [" + mostRecent + "]");
                    }
                }

            }
        };
    }

}
