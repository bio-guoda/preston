package bio.guoda.preston.cmd;

import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.process.StatementsListenerAdapter;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.KeyValueStore;
import bio.guoda.preston.store.HexaStoreImpl;
import bio.guoda.preston.store.HexaStoreReadOnly;
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

    public static void clone(KeyValueStore blobKeyValueStore, KeyValueStore provenanceLogKeyValueStore, KeyValueStore provenanceIndexKeyValueStore) {
        final BlobStoreReadOnly blobStore
                = new BlobStoreAppendOnly(blobKeyValueStore);

        final BlobStoreReadOnly provenanceLogStore
                = new BlobStoreAppendOnly(provenanceLogKeyValueStore);

        final HexaStoreReadOnly provenanceIndex
                = new HexaStoreImpl(provenanceIndexKeyValueStore);

        StatementsListener statementListener = blobToucher(blobStore);
        attemptReplay(provenanceLogStore,
                provenanceIndex,
                statementListener);
    }

    private static StatementsListener blobToucher(final BlobStoreReadOnly blobStore) {
        // touch blob, to let keyValueStore know about it
        return new StatementsListenerAdapter() {
            @Override
            public void on(Quad statement) {
                IRI mostRecent = VersionUtil.mostRecentVersionForStatement(statement);
                if (mostRecent != null) {
                    try (InputStream is = blobStore.get(mostRecent)) {
                    } catch (IOException e) {
                        LOG.warn("failed to copy [" + mostRecent + "]");
                    }
                }

            }
        };
    }

}
