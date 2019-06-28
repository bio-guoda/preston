package bio.guoda.preston.cmd;

import bio.guoda.preston.store.KeyTo3LevelPath;
import bio.guoda.preston.store.KeyTo5LevelPath;
import bio.guoda.preston.store.KeyToPath;
import bio.guoda.preston.store.KeyValueStore;
import bio.guoda.preston.store.KeyValueStoreLocalFileSystem;
import bio.guoda.preston.store.KeyValueStoreReadOnly;
import bio.guoda.preston.store.KeyValueStoreWithFallback;
import com.beust.jcommander.Parameter;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

public class PersistingLocal {

    @Parameter(names = {"--data-dir"}, description = "location of local content cache")
    private String localDataDir = "data";

    @Parameter(names = {"--tmp-dir"}, description = "location of local tmp dir")
    private String localTmpDir = "tmp";


    File getDefaultDataDir() {
        return getDataDir(getLocalDataDir());
    }

    File getTmpDir() {
        return getDataDir(getLocalTmpDir());
    }

    static File getDataDir(String data1) {
        File data = new File(data1);
        try {
            FileUtils.forceMkdir(data);
        } catch (IOException e) {
            //
        }
        return data;
    }

    protected KeyValueStore getKeyValueStore() {
        KeyValueStore primary = new KeyValueStoreLocalFileSystem(getTmpDir(), getKeyToPathLocal());
        KeyValueStoreReadOnly fallback = new KeyValueStoreLocalFileSystem(getTmpDir(), new KeyTo5LevelPath(getDefaultDataDir().toURI()));
        return new KeyValueStoreWithFallback(primary, fallback);
    }

    KeyToPath getKeyToPathLocal() {
        return new KeyTo3LevelPath(getDefaultDataDir().toURI());
    }

    public String getLocalDataDir() {
        return localDataDir;
    }

    public String getLocalTmpDir() {
        return localTmpDir;
    }


}
