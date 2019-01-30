package bio.guoda.preston.store;

import bio.guoda.preston.cmd.Persisting;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public class KeyValueStoreCopying implements KeyValueStore {
    private final KeyValueStore targetKeyValueStore;
    private final KeyValueStore sourceKeyValueStore;

    public KeyValueStoreCopying(String sourceDir, String targetDir, String tmpDir) {
        if (sourceDir.equals(targetDir)) {
            throw new IllegalArgumentException("source dir [" + sourceDir + "] must be different from target dir [" + targetDir + "].");
        }

        File source = Persisting.getDataDir(sourceDir);
        File target = Persisting.getDataDir(targetDir);
        File tmp = Persisting.getDataDir(tmpDir);


        this.sourceKeyValueStore = new KeyValueStoreLocalFileSystem(tmp, source);
        this.targetKeyValueStore = new KeyValueStoreLocalFileSystem(tmp, target);
    }

    public KeyValueStoreCopying(KeyValueStore source, KeyValueStore target) {
        this.sourceKeyValueStore = source;
        this.targetKeyValueStore = target;
    }

    @Override
    public void put(String key, String value) throws IOException {
        targetKeyValueStore.put(key, value);
    }

    @Override
    public String put(KeyGeneratingStream keyGeneratingStream, InputStream is) throws IOException {
        return targetKeyValueStore.put(keyGeneratingStream, is);
    }

    @Override
    public void put(String key, InputStream is) throws IOException {
        targetKeyValueStore.put(key, is);
    }

    @Override
    public InputStream get(String key) throws IOException {
        InputStream is = sourceKeyValueStore.get(key);
        if (is != null) {
            targetKeyValueStore.put(key, is);
            is.close();
            is = targetKeyValueStore.get(key);
        }
        return is;
    }
}
