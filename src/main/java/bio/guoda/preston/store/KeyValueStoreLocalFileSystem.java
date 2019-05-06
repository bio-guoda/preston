package bio.guoda.preston.store;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;


public class KeyValueStoreLocalFileSystem implements KeyValueStore {

    private final File tmpDir;
    private final KeyToPath keyToPath;

    public KeyValueStoreLocalFileSystem(File tmpDir, KeyToPath keyToPath) {
        this.tmpDir = tmpDir;
        this.keyToPath = keyToPath;
    }

    private static File getDataFile(URI filePath) {
        return new File(filePath);
    }


    @Override
    public void put(String key, String value) throws IOException {
        put(key, IOUtils.toInputStream(value, StandardCharsets.UTF_8));
    }

    @Override
    public void put(String key, InputStream source) throws IOException {
        try (InputStream src = source) {
            URI filePath = keyToPath.toPath(key);
            if (!getDataFile(filePath).exists()) {
                File destFile = getDataFile(filePath);
                File tmpDestFile = getDataFile(filePath.resolve(".tmp"));
                tmpDestFile.deleteOnExit();
                FileUtils.forceMkdirParent(destFile);
                try {
                    FileUtils.copyToFile(src, tmpDestFile);
                    tmpDestFile.renameTo(destFile);
                } catch (IOException ex) {
                    FileUtils.deleteQuietly(tmpDestFile);
                    FileUtils.deleteQuietly(destFile);
                    throw ex;
                }
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
        File dataFile = getDataFile(keyToPath.toPath(key));
        return dataFile.exists() ? FileUtils.openInputStream(dataFile) : null;
    }

}
