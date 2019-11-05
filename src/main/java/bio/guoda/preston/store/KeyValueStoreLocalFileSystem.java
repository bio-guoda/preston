package bio.guoda.preston.store;

import org.apache.commons.io.FileUtils;
import org.apache.commons.rdf.api.IRI;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;


public class KeyValueStoreLocalFileSystem implements KeyValueStore {

    private final File tmpDir;
    private final KeyToPath keyToPath;

    private final KeyValueStreamFactory keyValueStreamFactory;

    public KeyValueStoreLocalFileSystem(File tmpDir, KeyToPath keyToPath) {
        this(tmpDir, keyToPath, new AcceptingKeyValueStreamFactory());
    }

    public KeyValueStoreLocalFileSystem(File tmpDir, KeyToPath keyToPath, KeyValueStreamFactory keyValueStreamFactory) {
        this.tmpDir = tmpDir;
        this.keyToPath = keyToPath;
        this.keyValueStreamFactory = keyValueStreamFactory;
    }

    private static File getDataFile(URI filePath) {
        return new File(filePath);
    }


    @Override
    public void put(IRI key, InputStream value) throws IOException {
        try (InputStream is = value) {
            URI filePathBeforeCopy = keyToPath.toPath(key);
            if (!getDataFile(filePathBeforeCopy).exists()) {
                File tmpDestFile = getDataFile(URI.create(filePathBeforeCopy.toString() + ".tmp"));
                tmpDestFile.deleteOnExit();
                File destFile = null;
                try {
                    ValidatingKeyValueStream validating = getKeyValueStreamFactory().forKeyValueStream(key, is);
                    FileUtils.copyToFile(validating.getValueStream(), tmpDestFile);

                    if (validating.acceptValueStreamForKey(key)) {
                        URI filePathAfterCopy = keyToPath.toPath(key);
                        destFile = getDataFile(filePathAfterCopy);
                        if (!destFile.exists()) {
                            FileUtils.forceMkdirParent(destFile);
                            if (!tmpDestFile.renameTo(destFile)) {
                                throw new IOException("failed to rename tmp file from [" + tmpDestFile.getAbsolutePath() + "] to [" + destFile.getAbsolutePath() + "]");
                            }
                        }
                    }

                } catch (IOException ex) {
                    FileUtils.deleteQuietly(tmpDestFile);
                    FileUtils.deleteQuietly(destFile);
                    throw ex;
                }
            }
        } finally {
            value.close();
        }
    }


    /**
     * @param keyGeneratingStream
     * @param value               the caller is responsible for closing the inputstream
     * @return
     * @throws IOException
     */
    public IRI put(KeyGeneratingStream keyGeneratingStream, InputStream value) throws IOException {
        FileUtils.forceMkdir(tmpDir);
        File tmpFile = File.createTempFile("cacheFile", ".tmp", tmpDir);
        FileOutputStream os = FileUtils.openOutputStream(tmpFile);
        IRI key = keyGeneratingStream.generateKeyWhileStreaming(value, os);
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

    private KeyValueStreamFactory getKeyValueStreamFactory() {
        return keyValueStreamFactory;
    }

    public static class AcceptingKeyValueStreamFactory implements KeyValueStreamFactory {
        @Override
        public ValidatingKeyValueStream forKeyValueStream(IRI key, InputStream is) {
            return new ValidatingKeyValueStream() {
                @Override
                public InputStream getValueStream() {
                    return is;
                }

                @Override
                public boolean acceptValueStreamForKey(IRI key) {
                    return true;
                }
            };
        }
    }

    public static class KeyValueStreamFactorySHA256Values implements KeyValueStreamFactory {
        @Override
        public ValidatingKeyValueStream forKeyValueStream(IRI key, InputStream is) {
            return new ValidatingKeyValueStreamSHA256IRI(key, is);
        }
    }
}
