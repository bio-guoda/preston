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
import bio.guoda.preston.store.ProvenanceTracker;
import bio.guoda.preston.store.ProvenanceTrackerImpl;
import com.beust.jcommander.Parameter;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

public class PersistingLocal extends Cmd {

    @Parameter(names = {"--data-dir"}, description = "location of local content cache")
    private String localDataDir = "data";

    @Parameter(names = {"--tmp-dir"}, description = "location of local tmp dir")
    private String localTmpDir = "tmp";

    @Parameter(names = {"--hash-algorithm", "--algo", "-a"}, description = "hash algorithm used to generate primary content identifiers")
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

    protected ProvenanceTracker getProvenanceTracker() {
        KeyValueStore keyValueStore = getKeyValueStore(new KeyValueStoreLocalFileSystem.KeyValueStreamFactoryValues(getHashType()));
        return new ProvenanceTrackerImpl(new HexaStoreImpl(
                keyValueStore,
                getHashType()
        ));
    }

    protected ProvenanceTracker getProvenanceTracker(KeyValueStore keyValueStore) {
        return new ProvenanceTrackerImpl(
                new HexaStoreImpl(
                        keyValueStore,
                        getHashType()
                ));
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
