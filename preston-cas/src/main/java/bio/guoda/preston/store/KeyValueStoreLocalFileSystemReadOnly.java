package bio.guoda.preston.store;

import org.apache.commons.io.FileUtils;
import org.apache.commons.rdf.api.IRI;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public class KeyValueStoreLocalFileSystemReadOnly implements KeyValueStoreReadOnly {
    private final KeyToPath keyToPath;

    public KeyValueStoreLocalFileSystemReadOnly(KeyToPath keyToPath) {
        this.keyToPath = keyToPath;
    }

    @Override
    public InputStream get(IRI key) throws IOException {
        File dataFile = null;
        if (keyToPath.supports(key)) {
            dataFile = getDataFile(getPathForKey(key));
        }
        return dataFile != null && dataFile.exists()
                ? FileUtils.openInputStream(dataFile)
                : null;
    }

    URI getPathForKey(IRI key) {
        return keyToPath.toPath(key);
    }

    static File getDataFile(URI filePath) {
        return new File(filePath);
    }


}
