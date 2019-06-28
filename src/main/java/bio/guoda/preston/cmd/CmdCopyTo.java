package bio.guoda.preston.cmd;

import bio.guoda.preston.store.KeyTo3LevelPath;
import bio.guoda.preston.store.KeyValueStore;
import bio.guoda.preston.store.KeyValueStoreCopying;
import bio.guoda.preston.store.KeyValueStoreLocalFileSystem;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import java.io.File;

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
