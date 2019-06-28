package bio.guoda.preston.cmd;

import bio.guoda.preston.model.RefNodeFactory;
import bio.guoda.preston.process.StatementListener;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.KeyTo3LevelPath;
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

    @Parameter(description = "[target directory]")
    private String targetDir;

    @Override
    public void run() {
        File source = getDefaultDataDir();
        File target = Persisting.getDataDir(targetDir);
        if (source.equals(target)) {
            throw new IllegalArgumentException("source dir [" + source.getAbsolutePath() + "] must be different from target dir [" + target.getAbsolutePath() + "].");
        }
        File tmp = getTmpDir();

        KeyValueStore copyingKeyValueStore = new KeyValueStoreCopying(
                getKeyValueStore(),
                new KeyValueStoreLocalFileSystem(tmp, new KeyTo3LevelPath(target.toURI())));

        CloneUtil.clone(copyingKeyValueStore);
    }

}
