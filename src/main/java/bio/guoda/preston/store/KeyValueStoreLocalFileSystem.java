package bio.guoda.preston.store;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.input.CountingInputStream;
import org.apache.commons.rdf.api.IRI;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.concurrent.atomic.AtomicLong;


public class KeyValueStoreLocalFileSystem implements KeyValueStore {

    private final File tmpDir;
    private final KeyToPath keyToPath;
    private final KeyValueStoreListener listener;
    private AtomicLong totalBytes = new AtomicLong(0);
    private AtomicLong totalKeys = new AtomicLong(0L);

    public KeyValueStoreLocalFileSystem(File tmpDir, KeyToPath keyToPath) {
        this(tmpDir, keyToPath, new KeyValueStoreListener() {
            @Override
            public void beforePut(IRI key, long valueSizeInBytes) {

            }

            @Override
            public void afterPut(IRI key, long valueSizeInBytes) {

            }
        });
    }

    public KeyValueStoreLocalFileSystem(File tmpDir, KeyToPath keyToPath, KeyValueStoreListener listener) {
        this.tmpDir = tmpDir;
        this.keyToPath = keyToPath;
        this.listener = listener;
    }

    private static File getDataFile(URI filePath) {
        return new File(filePath);
    }


    interface KeyValueStoreListener {
        void beforePut(IRI key, long valueSizeInBytes);
        void afterPut(IRI key, long valueSizeInBytes);
    }

    @Override
    public void put(IRI key, InputStream source) throws IOException {
        try (InputStream is = source) {
            URI filePathBeforeCopy = keyToPath.toPath(key);
            if (!getDataFile(filePathBeforeCopy).exists()) {
                File tmpDestFile = getDataFile(URI.create(filePathBeforeCopy.toString() + ".tmp"));
                tmpDestFile.deleteOnExit();
                File destFile = null;
                try {
                    CountingInputStream isCounting = new CountingInputStream(is);
                    FileUtils.copyToFile(isCounting, tmpDestFile);

                    listener.beforePut(key, isCounting.getByteCount());

                    URI filePathAfterCopy = keyToPath.toPath(key);
                    destFile = getDataFile(filePathAfterCopy);
                    if (!destFile.exists()) {
                        FileUtils.forceMkdirParent(destFile);
                        if (!tmpDestFile.renameTo(destFile)) {
                            throw new IOException("failed to rename tmp file from [" + tmpDestFile.getAbsolutePath() + "] to [" + destFile.getAbsolutePath() + "]");
                        }
                    }

                    listener.afterPut(key, isCounting.getByteCount());

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
    public IRI put(KeyGeneratingStream keyGeneratingStream, InputStream is) throws IOException {
        FileUtils.forceMkdir(tmpDir);
        File tmpFile = File.createTempFile("cacheFile", ".tmp", tmpDir);
        FileOutputStream os = FileUtils.openOutputStream(tmpFile);
        IRI key = keyGeneratingStream.generateKeyWhileStreaming(is, os);
        try (InputStream tmpIs = FileUtils.openInputStream(tmpFile)) {
            put(key, tmpIs);
        } finally {
            FileUtils.deleteQuietly(tmpFile);
        }
        return key;
    }

    @Override
    public InputStream get(IRI key) throws IOException {
        File dataFile = getDataFile(keyToPath.toPath(key));
        return dataFile.exists() ? FileUtils.openInputStream(dataFile) : null;
    }

}
