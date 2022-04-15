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
import bio.guoda.preston.store.KeyValueStreamFactory;
import bio.guoda.preston.store.ProvenanceTracer;
import bio.guoda.preston.store.TracerOfDescendants;
import bio.guoda.preston.store.TracerOfOrigins;
import com.beust.jcommander.Parameter;
import org.apache.commons.io.FileUtils;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;

public class PersistingLocal extends Cmd {

    public static final String LOCATION_OF_LOCAL_CONTENT_CACHE = "Location of local content cache";
    public static final String LOCATION_OF_LOCAL_TMP_DIR = "Location of local tmp dir";
    public static final String HASH_ALGORITHM_USED_TO_GENERATE_PRIMARY_CONTENT_IDENTIFIERS = "Hash algorithm used to generate primary content identifiers";
    @Parameter(names = {"--data-dir"}, description = LOCATION_OF_LOCAL_CONTENT_CACHE)

    @CommandLine.Option(
            names = {"--data-dir"},
            description = LOCATION_OF_LOCAL_CONTENT_CACHE
    )
    private String localDataDir = "data";

    @Parameter(names = {"--tmp-dir"}, description = LOCATION_OF_LOCAL_TMP_DIR)
    @CommandLine.Option(
            names = {"--tmp-dir"},
            description = LOCATION_OF_LOCAL_TMP_DIR
    )
    private String localTmpDir = "tmp";

    @Parameter(names = {"--hash-algorithm", "--algo", "-a"}, description = HASH_ALGORITHM_USED_TO_GENERATE_PRIMARY_CONTENT_IDENTIFIERS)
    @CommandLine.Option(
            names = {"--hash-algorithm", "--algo", "-a"},
            description = HASH_ALGORITHM_USED_TO_GENERATE_PRIMARY_CONTENT_IDENTIFIERS
    )
    private HashType hashType = HashType.sha256;


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

    protected KeyValueStore getKeyValueStore(KeyValueStreamFactory keyValueStreamFactory) {
        KeyValueStore primary = new KeyValueStoreLocalFileSystem(getTmpDir(), getKeyToPathLocal(), keyValueStreamFactory);
        KeyValueStoreReadOnly fallback = new KeyValueStoreLocalFileSystem(getTmpDir(), new KeyTo5LevelPath(getDefaultDataDir().toURI(), getHashType()), keyValueStreamFactory);
        return new KeyValueStoreWithFallback(primary, fallback);
    }

    protected ProvenanceTracer getTracerOfDescendants() {
        KeyValueStore keyValueStore = getKeyValueStore(
                new KeyValueStoreLocalFileSystem.KeyValueStreamFactoryValues(getHashType())
        );

        HexaStoreImpl hexastore = new HexaStoreImpl(
                keyValueStore,
                getHashType()
        );

        return new TracerOfDescendants(hexastore, this);
    }

    protected ProvenanceTracer getTracerOfOrigins(KeyValueStore keyValueStore) {
        return new TracerOfOrigins(keyValueStore, this);
    }


    KeyToPath getKeyToPathLocal() {
        return new KeyTo3LevelPath(getDefaultDataDir().toURI(), getHashType());
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
