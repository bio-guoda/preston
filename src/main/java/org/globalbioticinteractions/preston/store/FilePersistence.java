package org.globalbioticinteractions.preston.store;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.globalbioticinteractions.preston.Hasher;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class FilePersistence implements Persistence {

    private final File tmpDir;
    private final File datasetDir;

    public FilePersistence() {
        this(new File("tmp"), new File("datasets"));
    }

    public FilePersistence(File tmpDir, File datasetDir) {
        this.tmpDir = tmpDir;
        this.datasetDir = datasetDir;
    }

    public static File getDataFile(String key, File dataDir) {
        return new File(getDatasetDir(key, dataDir), "data");
    }

    public static File getDatasetDir(String key, File dataDir) {
        return new File(dataDir, toPath(key));
    }

    public static String toPath(String key) {
        int offset = Hasher.getHashPrefix().length();
        int expectedLength = 8 + offset;
        if (StringUtils.length(key) < expectedLength) {
            throw new IllegalArgumentException("expected id [" + key + "] of at least [" + expectedLength + "] characters");
        }
        String u0 = key.substring(offset + 0, offset + 2);
        String u1 = key.substring(offset + 2, offset + 4);
        String u2 = key.substring(offset + 4, offset + 6);
        return StringUtils.join(Arrays.asList(u0, u1, u2, key.substring(offset)), "/");
    }


    @Override
    public void put(String key, String value) throws IOException {
        try (InputStream source = IOUtils.toInputStream(value, StandardCharsets.UTF_8)) {
            writeToDiskIfNotExists(key, source);
        }
    }

    private void writeToDiskIfNotExists(String key, InputStream source) throws IOException {
        if (!getDataFile(key, getDatasetDir()).exists()) {
            File datasetPath = getDatasetDir(key, getDatasetDir());
            FileUtils.forceMkdir(datasetPath);
            File destFile = getDataFile(key, getDatasetDir());
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
        File dataFile = getDataFile(key, getDatasetDir());
        return dataFile.exists() ? FileUtils.openInputStream(dataFile) : null;
    }

    private File getDatasetDir() {
        return datasetDir;
    }

}
