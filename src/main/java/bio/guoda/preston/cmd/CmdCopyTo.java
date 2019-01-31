package bio.guoda.preston.cmd;

import bio.guoda.preston.model.RefNodeFactory;
import bio.guoda.preston.process.StatementListener;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.KeyValueStoreCopying;
import bio.guoda.preston.store.KeyValueStore;
import bio.guoda.preston.store.KeyValueStoreLocalFileSystem;
import bio.guoda.preston.store.StatementStoreImpl;
import bio.guoda.preston.store.VersionUtil;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Triple;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static bio.guoda.preston.cmd.ReplayUtil.attemptReplay;
import static bio.guoda.preston.model.RefNodeFactory.toBlank;

@Parameters(separators = "= ", commandDescription = "Copy biodiversity dataset graph")
public class CmdCopyTo extends LoggingPersisting implements Runnable {

    private static final Log LOG = LogFactory.getLog(CmdCopyTo.class);

    @Parameter(description = "[target directory]")
    private String targetDir;

    @Override
    public void run() {
        String sourceDir = "data";
        String tmpDir = "tmp";
        if (sourceDir.equals(targetDir)) {
            throw new IllegalArgumentException("source dir [" + sourceDir + "] must be different from target dir [" + targetDir + "].");
        }

        File source = Persisting.getDataDir(sourceDir);
        File target = Persisting.getDataDir(targetDir);
        File tmp = Persisting.getDataDir(tmpDir);

        KeyValueStore copyingKeyValueStore = new KeyValueStoreCopying(
                new KeyValueStoreLocalFileSystem(tmp, source),
                new KeyValueStoreLocalFileSystem(tmp, target));
        final BlobStoreAppendOnly blobStore = new BlobStoreAppendOnly(copyingKeyValueStore);
        Set<String> IRIStrings = new TreeSet<>();

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        System.out.print("indexing...");
        attemptReplay(blobStore,
                new StatementStoreImpl(copyingKeyValueStore),
                new StatementListener() {
                    @Override
                    public void on(Triple statement) {
                        IRI mostRecent = VersionUtil.mostRecentVersionForStatement(statement);
                        if (mostRecent != null) {
                            IRIStrings.add(mostRecent.getIRIString());
                        }
                    }
                });
        System.out.println(" done.");


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
        stopWatch.stop();
        System.out.println("\tcopying done.");
        System.out.println("Copied [" + IRIStrings.size() + "] datasets from [" + source.getAbsolutePath() + "] to [" + targetDir + "] in [" + stopWatch.getTime(TimeUnit.MINUTES) + "] minutes.");
    }

    static String formatProgress(long i, long total) {
        return String.format("\rcopying [%02.1f%%]...", (100.0 * i / total));
    }

}
