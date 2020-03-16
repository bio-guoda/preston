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
            URI filePathBeforeCopy = getPathForKey(key);
            if (!getDataFile(filePathBeforeCopy).exists()) {
                File tmpDestFile = getDataFile(URI.create(filePathBeforeCopy.toString() + ".tmp"));
                tmpDestFile.deleteOnExit();
                File destFile = null;
                try {
                    ValidatingKeyValueStream validating = getKeyValueStreamFactory().forKeyValueStream(key, is);
                    FileUtils.copyToFile(validating.getValueStream(), tmpDestFile);

                    if (validating.acceptValueStreamForKey(key)) {
                        put(key, tmpDestFile);
                    } else {
                        FileUtils.deleteQuietly(tmpDestFile);
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

    private URI getPathForKey(IRI key) {
        return keyToPath.toPath(key);
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
        IRI key;
        try (FileOutputStream os = FileUtils.openOutputStream(tmpFile)) {
            key = keyGeneratingStream.generateKeyWhileStreaming(value, os);
            os.flush();
        }
        try {
            put(key, tmpFile);
        } finally {
            FileUtils.deleteQuietly(tmpFile);
        }
        return key;
    }

    @Override
    public InputStream get(IRI key) throws IOException {
        File dataFile = getDataFile(getPathForKey(key));
        return dataFile.exists() ? FileUtils.openInputStream(dataFile) : null;
    }

    private void put(IRI key, File tmpDestFile) throws IOException {
        if (!tmpDestFile.exists()) {
            throw new IOException("cannot store a file that does not exist");
        }
        URI filePathAfterCopy = getPathForKey(key);
        File destFile = getDataFile(filePathAfterCopy);
        if (!destFile.exists()) {
            FileUtils.forceMkdirParent(destFile);
            FileUtils.moveFile(tmpDestFile, destFile);
        }
    }

    private KeyValueStreamFactory getKeyValueStreamFactory() {
        return keyValueStreamFactory;
    }

    public static class ValidatingKeyValueStreamContentAddressedFactory implements KeyValueStreamFactory {
        @Override
        public ValidatingKeyValueStream forKeyValueStream(IRI key, InputStream is) {
            return new ValidatingKeyValueStreamContentAddressed(is);
        }
    }

    public static class KeyValueStreamFactorySHA256Values implements KeyValueStreamFactory {
        @Override
        public ValidatingKeyValueStream forKeyValueStream(IRI key, InputStream is) {
            return new ValidatingKeyValueStreamSHA256IRI(is);
        }
    }

}
