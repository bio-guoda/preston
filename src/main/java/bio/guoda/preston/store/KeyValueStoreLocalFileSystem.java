package bio.guoda.preston.store;

import jdk.internal.util.xml.impl.Input;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;


public class KeyValueStoreLocalFileSystem implements KeyValueStore {

    private final File tmpDir;
    private final File datasetDir;
    private final KeyToPath keyToPath;

    public KeyValueStoreLocalFileSystem(File tmpDir, File datasetDir) {
        this(tmpDir, datasetDir, new KeyTo5LevelPath());
    }

    public KeyValueStoreLocalFileSystem(File tmpDir, File datasetDir, KeyToPath keyToPath) {
        this.tmpDir = tmpDir;
        this.datasetDir = datasetDir;
        this.keyToPath = keyToPath;
    }

    public static File getDataFile(File parentDir, String filePath) {
        return new File(parentDir, filePath);
    }


    @Override
    public void put(String key, String value) throws IOException {
        put(key, IOUtils.toInputStream(value, StandardCharsets.UTF_8));
    }

    @Override
    public void put(String key, InputStream source) throws IOException {
        try (InputStream src = source) {
            String filePath = keyToPath.toPath(key);
            if (!getDataFile(getDatasetDir(), filePath).exists()) {
                File destFile = getDataFile(getDatasetDir(), filePath);
                FileUtils.forceMkdirParent(destFile);
                FileUtils.copyToFile(src, destFile);
            }
        }
    }

    @Override
    public String put(KeyGeneratingStream keyGeneratingStream, InputStream is) throws IOException {
        FileUtils.forceMkdir(tmpDir);
        File tmpFile = File.createTempFile("cacheFile", ".tmp", tmpDir);
        FileOutputStream os = FileUtils.openOutputStream(tmpFile);
        String key = keyGeneratingStream.generateKeyWhileStreaming(is, os);
        try {
            put(key, FileUtils.openInputStream(tmpFile));
        } finally {
            FileUtils.deleteQuietly(tmpFile);
        }
        return key;
    }

    @Override
    public InputStream get(String key) throws IOException {
        File dataFile = getDataFile(getDatasetDir(), keyToPath.toPath(key));
        return dataFile.exists() ? FileUtils.openInputStream(dataFile) : null;
    }

    private File getDatasetDir() {
        return datasetDir;
    }

}
