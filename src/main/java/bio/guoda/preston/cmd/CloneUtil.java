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

    public static Set<String> clone(KeyValueStore keyValueStore) {
        System.err.print("indexing...");
        final BlobStoreAppendOnly blobStore = new BlobStoreAppendOnly(keyValueStore);
        Set<String> IRIStrings = new TreeSet<>();
        attemptReplay(blobStore,
                new StatementStoreImpl(keyValueStore),
                new StatementListener() {
                    @Override
                    public void on(Triple statement) {
                        IRI mostRecent = VersionUtil.mostRecentVersionForStatement(statement);
                        if (mostRecent != null) {
                            IRIStrings.add(mostRecent.getIRIString());
                        }
                    }
                });
        System.err.println(" done.");


        AtomicLong count = new AtomicLong(0L);
        for (String IRIString : IRIStrings) {
            try (InputStream is = blobStore.get(RefNodeFactory.toIRI(IRIString))) {
                long l = count.incrementAndGet();
                if (l % 100 == 0) {
                    System.out.print(formatProgress(l, IRIStrings.size()));
                }
            } catch (IOException e) {
                LOG.warn("failed to copy [" + IRIString + "]");
            }
        }
        return IRIStrings;
    }

    static String formatProgress(long i, long total) {
        return String.format("\rcopying [%02.1f%%]...", (100.0 * i / total));
    }
}
