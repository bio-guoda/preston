package bio.guoda.preston.cmd;

import bio.guoda.preston.store.FileKeyValueStore;
import bio.guoda.preston.store.KeyValueStore;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

public class Persisting {

    KeyValueStore getBlobPersistence(File dataDir) {
        return new FileKeyValueStore(getTmpDir(), dataDir);
    }

    KeyValueStore getBlobPersistence() {
        return getBlobPersistence(getDefaultDataDir());
    }

    public File getDefaultDataDir() {
        return getDataDir("data");
    }

    KeyValueStore getCrawlRelationsStore() {
        return getBlobPersistence(getDefaultDataDir());
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
