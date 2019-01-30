package bio.guoda.preston.cmd;

import bio.guoda.preston.store.KeyValueStoreLocalFileSystem;
import bio.guoda.preston.store.KeyValueStore;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

public class Persisting {

    KeyValueStore getKeyValueStore(File dataDir) {
        return new KeyValueStoreLocalFileSystem(getTmpDir(), dataDir);
    }

    KeyValueStore getKeyValueStore() {
        return getKeyValueStore(getDefaultDataDir());
    }

    public File getDefaultDataDir() {
        return getDataDir("data");
    }

    File getTmpDir() {
        return getDataDir("tmp");
    }

    public static File getDataDir(String data1) {
        File data = new File(data1);
        try {
            FileUtils.forceMkdir(data);
        } catch (IOException e) {
            //
        }
        return data;
    }

}
