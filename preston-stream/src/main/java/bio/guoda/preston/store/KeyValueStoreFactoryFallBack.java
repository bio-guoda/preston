package bio.guoda.preston.store;

import java.io.File;

public class KeyValueStoreFactoryFallBack implements KeyValueStoreFactory {

    private final File dataDir;
    private final File tmpDir;
    private KeyToPath keyToPath;

    public KeyValueStoreFactoryFallBack(File dataDir, File tmpDir, int directoryDepth) {
        this.dataDir = dataDir;
        this.tmpDir = tmpDir;
        this.keyToPath = new KeyToPathFactoryDepth(dataDir.toURI(), directoryDepth).getKeyToPath();

    }

    public KeyValueStoreFactoryFallBack(KeyValueStoreConfig config) {
        this(config.getDataDir(), config.getTmpDir(), config.getDirectoryDepth());
    }

    @Override
    public KeyValueStore getKeyValueStore(ValidatingKeyValueStreamFactory validatingKeyValueStreamFactory) {
        KeyValueStore primary = new KeyValueStoreLocalFileSystem(
                this.tmpDir,
                this.keyToPath,
                validatingKeyValueStreamFactory
        );

        // for backwards compatibility
        KeyValueStoreReadOnly fallback = new KeyValueStoreLocalFileSystem(
                this.tmpDir,
                new KeyTo5LevelPath(this.dataDir.toURI()),
                validatingKeyValueStreamFactory
        );

        return new KeyValueStoreWithFallback(primary, fallback);
    }

}
