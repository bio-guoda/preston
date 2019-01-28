package bio.guoda.preston.cmd;

import bio.guoda.preston.store.FileKeyValueStore;
import bio.guoda.preston.store.KeyValueStore;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

public class Persisting {

    KeyValueStore getBlobPersistence() {
        return new FileKeyValueStore(getTmpDir(), getDataDir());
    }

    KeyValueStore getDatasetRelationsStore() {
        return new FileKeyValueStore(getTmpDir(), getDataDir());
    }

    KeyValueStore getLogRelationsStore() {
        return new FileKeyValueStore(getTmpDir(), getDataDir());
    }

    File getTmpDir() {
        File tmp = new File("tmp");
        try {
            FileUtils.forceMkdir(tmp);
        } catch (IOException e) {
            //
        }
        return tmp;
    }

    File getDataDir() {
        File data = new File("data");
        try {
            FileUtils.forceMkdir(data);
        } catch (IOException e) {
            //
        }
        return data;
    }

}
