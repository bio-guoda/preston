package bio.guoda.preston.cmd;

import bio.guoda.preston.store.KeyTo5LevelPath;
import bio.guoda.preston.store.KeyValueStoreLocalFileSystem;
import bio.guoda.preston.store.KeyValueStore;
import bio.guoda.preston.store.KeyValueStoreReadOnly;
import bio.guoda.preston.store.KeyValueStoreWithFallback;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

public class Persisting {

    KeyValueStore getKeyValueStore() {
        KeyValueStore primary = new KeyValueStoreLocalFileSystem(getTmpDir(), getDefaultDataDir());
        KeyValueStoreReadOnly fallback = new KeyValueStoreLocalFileSystem(getTmpDir(), getDefaultDataDir(), new KeyTo5LevelPath());
        return new KeyValueStoreWithFallback(primary, fallback);
    }

    File getDefaultDataDir() {
        return getDataDir("data");
    }

    File getTmpDir() {
        return getDataDir("tmp");
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

}
