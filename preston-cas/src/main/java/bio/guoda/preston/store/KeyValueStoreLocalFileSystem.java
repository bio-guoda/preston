package bio.guoda.preston.store;

import org.apache.commons.io.FileUtils;
import org.apache.commons.rdf.api.IRI;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;


public class KeyValueStoreLocalFileSystem extends KeyValueStoreLocalFileSystemReadOnly implements KeyValueStoreWithRemove {

    private final File tmpDir;

    private final ValidatingKeyValueStreamFactory validatingKeyValueStreamFactory;

    public KeyValueStoreLocalFileSystem(File tmpDir, KeyToPath keyToPath, ValidatingKeyValueStreamFactory validatingKeyValueStreamFactory) {
        super(keyToPath);
        this.tmpDir = tmpDir;
        this.validatingKeyValueStreamFactory = validatingKeyValueStreamFactory;
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
                    ValidatingKeyValueStream validating = getValidatingKeyValueStreamFactory().forKeyValueStream(key, is);
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


    private void put(IRI key, File tmpDestFile) throws IOException {
        if (!tmpDestFile.exists()) {
            throw new IOException("cannot store a file that does not exist");
        }
        File destFile = getDestFileForKey(key);
        if (!destFile.exists()) {
            FileUtils.forceMkdirParent(destFile);
            FileUtils.moveFile(tmpDestFile, destFile);
        }
    }

    private ValidatingKeyValueStreamFactory getValidatingKeyValueStreamFactory() {
        return validatingKeyValueStreamFactory;
    }

    @Override
    public void remove(IRI key) throws IOException {
        File destFile = getDestFileForKey(key);
        if (destFile.exists()) {
            FileUtils.delete(destFile);
        }
    }

    private File getDestFileForKey(IRI key) {
        URI filePathAfterCopy = getPathForKey(key);
        return getDataFile(filePathAfterCopy);
    }
}
