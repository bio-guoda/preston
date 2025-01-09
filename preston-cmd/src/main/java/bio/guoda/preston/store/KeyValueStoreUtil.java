package bio.guoda.preston.store;

import bio.guoda.preston.cmd.KeyToPathFactory;
import bio.guoda.preston.cmd.KeyToPathFactoryDepth;
import bio.guoda.preston.cmd.KeyValueStoreFactoryFallBack;

import java.io.File;

public class KeyValueStoreUtil {

    public static KeyValueStore getKeyValueStore(
            final ValidatingKeyValueStreamFactory validatingKeyValueStreamFactory,
            final File dataDir,
            final File tmpDir,
            int directoryDepth) {

        final KeyToPathFactory keyToPathFactory
                = new KeyToPathFactoryDepth(dataDir.toURI(), directoryDepth);

        return new KeyValueStoreFactoryFallBack(
                dataDir,
                tmpDir,
                keyToPathFactory.getKeyToPath()
        ).getKeyValueStore(validatingKeyValueStreamFactory);
    }
}
