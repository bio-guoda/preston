package bio.guoda.preston.cmd;

import bio.guoda.preston.HashType;
import bio.guoda.preston.store.HexaStoreImpl;
import bio.guoda.preston.store.KeyTo3LevelPath;
import bio.guoda.preston.store.KeyTo5LevelPath;
import bio.guoda.preston.store.KeyToPath;
import bio.guoda.preston.store.KeyValueStore;
import bio.guoda.preston.store.KeyValueStoreLocalFileSystem;
import bio.guoda.preston.store.KeyValueStoreReadOnly;
import bio.guoda.preston.store.KeyValueStoreWithFallback;
import bio.guoda.preston.store.ValidatingKeyValueStreamFactory;
import bio.guoda.preston.store.ValidatingKeyValueStreamHashTypeIRIFactory;
import bio.guoda.preston.store.ProvenanceTracer;
import bio.guoda.preston.store.TracerOfDescendants;
import bio.guoda.preston.store.TracerOfOrigins;
import org.apache.commons.io.FileUtils;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;

public class PersistingLocal extends Cmd {

    public static final String LOCATION_OF_LOCAL_CONTENT_CACHE = "Location of local content cache";
    public static final String LOCATION_OF_LOCAL_TMP_DIR = "Location of local tmp dir";
    public static final String HASH_ALGORITHM_USED_TO_GENERATE_PRIMARY_CONTENT_IDENTIFIERS = "Hash algorithm used to generate primary content identifiers";

    @CommandLine.Option(
            names = {"--data-dir"},
            description = "Location of local content cache"
    )
    private String localDataDir = "data";

    @CommandLine.Option(
            names = {"--tmp-dir"},
            description = "Location of local tmp dir"
    )
    private String localTmpDir = "tmp";

    @CommandLine.Option(
            names = {"--hash-algorithm", "--algo", "-a"},
            description = "Hash algorithm used to generate primary content identifiers"
    )
    private HashType hashType = HashType.sha256;
    private KeyToPath keyToPathLocal = null;


    File getDefaultDataDir() {
        return getDataDir(getLocalDataDir());
    }

    File getTmpDir() {
        return getDataDir(getLocalTmpDir());
    }

    static File getDataDir(String data1) {
        File data = new File(data1);
        try {
            FileUtils.forceMkdir(data);
        } catch (IOException e) {
            //
        }
        return data;
    }

    protected KeyValueStore getKeyValueStore(ValidatingKeyValueStreamFactory validatingKeyValueStreamFactory) {
        KeyValueStore primary = new KeyValueStoreLocalFileSystem(getTmpDir(), getKeyToPathLocal(), validatingKeyValueStreamFactory);
        KeyValueStoreReadOnly fallback = new KeyValueStoreLocalFileSystem(getTmpDir(), new KeyTo5LevelPath(getDefaultDataDir().toURI(), getHashType()), validatingKeyValueStreamFactory);
        return new KeyValueStoreWithFallback(primary, fallback);
    }

    protected ProvenanceTracer getTracerOfDescendants() {
        KeyValueStore keyValueStore = getKeyValueStore(
                new ValidatingKeyValueStreamHashTypeIRIFactory(getHashType())
        );

        return getTracerOfDescendants(keyValueStore);
    }

    public ProvenanceTracer getTracerOfDescendants(KeyValueStore keyValueStore) {
        HexaStoreImpl hexastore = new HexaStoreImpl(
                keyValueStore,
                getHashType()
        );

        return new TracerOfDescendants(hexastore, this);
    }

    protected ProvenanceTracer getTracerOfOrigins(KeyValueStore keyValueStore) {
        return new TracerOfOrigins(keyValueStore, this);
    }


    public KeyToPath getKeyToPathLocal() {
        initIfNeeded();
        return keyToPathLocal;
    }

    public void setKeyToPathLocal(KeyToPath keyToPath) {
        this.keyToPathLocal = keyToPath;
    }

    private void initIfNeeded() {
        if (keyToPathLocal == null) {
            keyToPathLocal = new KeyTo3LevelPath(getDefaultDataDir().toURI(), getHashType());
        }
    }

    public String getLocalDataDir() {
        return localDataDir;
    }

    public String getLocalTmpDir() {
        return localTmpDir;
    }

    public void setLocalDataDir(String localDataDir) {
        this.localDataDir = localDataDir;
    }

    public void setLocalTmpDir(String localTmpDir) {
        this.localTmpDir = localTmpDir;
    }

    public HashType getHashType() {
        return hashType;
    }

    public void setHashType(HashType hashType) {
        this.hashType = hashType;
    }

}
