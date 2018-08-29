package org.globalbioticinteractions.preston.store;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.globalbioticinteractions.preston.process.BlobStoreWriter;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

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


    @Override
    public void put(String key, String value) throws IOException {
        try (InputStream source = IOUtils.toInputStream(value, StandardCharsets.UTF_8)) {
            writeToDiskIfNotExists(key, source);
        }
    }

    private void writeToDiskIfNotExists(String key, InputStream source) throws IOException {
        if (!BlobStoreWriter.getDataFile(key, getDatasetDir()).exists()) {
            File datasetPath = BlobStoreWriter.getDatasetDir(key, getDatasetDir());
            FileUtils.forceMkdir(datasetPath);
            File destFile = BlobStoreWriter.getDataFile(key, getDatasetDir());
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
        File dataFile = BlobStoreWriter.getDataFile(key, getDatasetDir());
        return dataFile.exists() ? FileUtils.openInputStream(dataFile) : null;
    }

    File getDatasetDir() {
        return datasetDir;
    }

}
