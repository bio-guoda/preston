package bio.guoda.preston.cmd;

import bio.guoda.preston.process.BlobStoreReadOnly;
import bio.guoda.preston.process.StatementListener;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.KeyValueStore;
import bio.guoda.preston.store.StatementStoreImpl;
import bio.guoda.preston.store.StatementStoreReadOnly;
import bio.guoda.preston.store.VersionUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;

import static bio.guoda.preston.cmd.ReplayUtil.attemptReplay;

public class CloneUtil {
    private static final Log LOG = LogFactory.getLog(CloneUtil.class);

    public static void clone(KeyValueStore blobKeyValueStore, KeyValueStore provenanceLogKeyValueStore, KeyValueStore provenanceIndexKeyValueStore) {
        final BlobStoreReadOnly blobStore
                = new BlobStoreAppendOnly(blobKeyValueStore);

        final BlobStoreReadOnly provenanceLogStore
                = new BlobStoreAppendOnly(provenanceLogKeyValueStore);

        final StatementStoreReadOnly provenanceIndex
                = new StatementStoreImpl(provenanceIndexKeyValueStore);

        StatementListener statementListener = blobToucher(blobStore);
        attemptReplay(provenanceLogStore,
                provenanceIndex,
                statementListener);
    }

    private static StatementListener blobToucher(final BlobStoreReadOnly blobStore) {
        // touch blob, to let keyValueStore know about it
        return (StatementListener) statement -> {
            IRI mostRecent = VersionUtil.mostRecentVersionForStatement(statement);
            if (mostRecent != null) {
                try (InputStream is = blobStore.get(mostRecent)) {
                } catch (IOException e) {
                    LOG.warn("failed to copy [" + mostRecent + "]");
                }
            }
        };
    }

}
