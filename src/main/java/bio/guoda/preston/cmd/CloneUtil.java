package bio.guoda.preston.cmd;

import bio.guoda.preston.model.RefNodeFactory;
import bio.guoda.preston.process.StatementListener;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.KeyValueStore;
import bio.guoda.preston.store.StatementStoreImpl;
import bio.guoda.preston.store.VersionUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Triple;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import static bio.guoda.preston.cmd.ReplayUtil.attemptReplay;

public class CloneUtil {
    private static final Log LOG = LogFactory.getLog(CloneUtil.class);

    public static void clone(KeyValueStore keyValueStore) {
        final BlobStoreAppendOnly blobStore = new BlobStoreAppendOnly(keyValueStore);
        attemptReplay(blobStore,
                new StatementStoreImpl(keyValueStore),
                new StatementListener() {
                    @Override
                    public void on(Triple statement) {
                        IRI mostRecent = VersionUtil.mostRecentVersionForStatement(statement);
                        if (mostRecent != null) {
                            try (InputStream is = blobStore.get(mostRecent)) {
                            } catch (IOException e) {
                                LOG.warn("failed to copy [" + mostRecent + "]");
                            }
                        }
                    }
                });
    }

}
