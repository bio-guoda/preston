package bio.guoda.preston.store;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;


public class FilePersistence implements Persistence {

    private final File tmpDir;
    private final File datasetDir;
    private final KeyToPath keyToPath;

    public FilePersistence(File tmpDir, File datasetDir) {
        this(tmpDir, datasetDir, new KeyTo4LevelPath());
    }

    public FilePersistence(File tmpDir, File datasetDir, KeyToPath keyToPath) {
        this.tmpDir = tmpDir;
        this.datasetDir = datasetDir;
        this.keyToPath = keyToPath;
    }

    public static File getDataFile(File parentDir, String filePath) {
        return new File(parentDir, filePath);
    }


    public static String toPath(String key) {
        return new KeyTo4LevelPath().toPath(key);
    }


    @Override
    public void put(String key, String value) throws IOException {
        try (InputStream source = IOUtils.toInputStream(value, StandardCharsets.UTF_8)) {
            writeToDiskIfNotExists(key, source);
        }
    }

    private void writeToDiskIfNotExists(String key, InputStream source) throws IOException {
        String filePath = keyToPath.toPath(key);
        if (!getDataFile(getDatasetDir(), filePath).exists()) {
            File destFile = getDataFile(getDatasetDir(), filePath);
            FileUtils.forceMkdirParent(destFile);
            FileUtils.copyToFile(source, destFile);
        }
    }

    @Override
    public String put(KeyGeneratingStream keyGeneratingStream, InputStream is) throws IOException {
        FileUtils.forceMkdir(tmpDir);
        File tmpFile = File.createTempFile("cacheFile", ".tmp", tmpDir);
        String key = keyGeneratingStream.generateKeyWhileStreaming(is, FileUtils.openOutputStream(tmpFile));
        try (InputStream source = FileUtils.openInputStream(tmpFile)) {
            writeToDiskIfNotExists(key, source);
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
