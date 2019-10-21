package bio.guoda.preston.store;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;


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
    public void put(String key, InputStream source) throws IOException {
        try (InputStream is = source) {
            URI filePath = keyToPath.toPath(key);
            if (!getDataFile(filePath).exists()) {
                File destFile = getDataFile(filePath);
                File tmpDestFile = getDataFile(URI.create(filePath.toString() + ".tmp"));
                tmpDestFile.deleteOnExit();
                FileUtils.forceMkdirParent(destFile);
                try {
                    FileUtils.copyToFile(is, tmpDestFile);
                    if (!tmpDestFile.renameTo(destFile)) {
                        throw new IOException("failed to rename tmp file from [" + tmpDestFile.getAbsolutePath() + "] to [" + destFile.getAbsolutePath() + "]");
                    }
                } catch (IOException ex) {
                    FileUtils.deleteQuietly(tmpDestFile);
                    FileUtils.deleteQuietly(destFile);
                    throw ex;
                }
            }
        } finally {
            source.close();
        }
    }


    /**
     * @param keyGeneratingStream
     * @param is the caller is responsible for closing the inputstream
     * @return
     * @throws IOException
     */
    public String put(KeyGeneratingStream keyGeneratingStream, InputStream is) throws IOException {
        FileUtils.forceMkdir(tmpDir);
        File tmpFile = File.createTempFile("cacheFile", ".tmp", tmpDir);
        FileOutputStream os = FileUtils.openOutputStream(tmpFile);
        String key = keyGeneratingStream.generateKeyWhileStreaming(is, os);
        try (InputStream tmpIs = FileUtils.openInputStream(tmpFile)) {
            put(key, tmpIs);
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
