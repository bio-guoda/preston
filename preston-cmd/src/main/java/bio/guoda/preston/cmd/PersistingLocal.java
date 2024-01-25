package bio.guoda.preston.cmd;

import bio.guoda.preston.HashType;
import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.store.HexaStoreImpl;
import bio.guoda.preston.store.KeyTo1LevelPath;
import bio.guoda.preston.store.KeyTo3LevelPath;
import bio.guoda.preston.store.KeyTo5LevelPath;
import bio.guoda.preston.store.KeyToPath;
import bio.guoda.preston.store.KeyValueStore;
import bio.guoda.preston.store.KeyValueStoreLocalFileSystem;
import bio.guoda.preston.store.KeyValueStoreReadOnly;
import bio.guoda.preston.store.KeyValueStoreWithFallback;
import bio.guoda.preston.store.ProvenanceTracer;
import bio.guoda.preston.store.ProvenanceTracerByIndex;
import bio.guoda.preston.store.ProvenanceTracerImpl;
import bio.guoda.preston.store.ValidatingKeyValueStreamContentAddressedFactory;
import bio.guoda.preston.store.ValidatingKeyValueStreamFactory;
import bio.guoda.preston.store.ValidatingKeyValueStreamHashTypeIRIFactory;
import org.apache.commons.collections4.Factory;
import org.apache.commons.io.FileUtils;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.net.URI;

public class PersistingLocal extends CmdWithProvenance {

    @CommandLine.Option(
            names = {"--data-dir"},
            defaultValue = "data",
            description = "Location of local content cache"
    )
    private String localDataDir = "data";

    @CommandLine.Option(
            names = {"-d", "--depth"},
            defaultValue = "2",
            description = "folder depth of data dir"
    )
    private int depth = 2;

    @CommandLine.Option(
            names = {"--tmp-dir"},
            defaultValue = "tmp",
            description = "Location of local tmp dir"
    )
    private String localTmpDir = "tmp";

    @CommandLine.Option(
            names = {"--hash-algorithm", "--algo", "-a"},
            description = "Hash algorithm used to generate primary content identifiers. Supported values: ${COMPLETION-CANDIDATES}."
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
        KeyValueStore primary = new KeyValueStoreLocalFileSystem(
                getTmpDir(),
                getKeyToPathLocal(getDefaultDataDir().toURI()),
                validatingKeyValueStreamFactory
        );

        KeyValueStoreReadOnly fallback = new KeyValueStoreLocalFileSystem(
                getTmpDir(),
                new KeyTo5LevelPath(getDefaultDataDir().toURI()),
                validatingKeyValueStreamFactory
        );

        return new KeyValueStoreWithFallback(primary, fallback);
    }


    public ProvenanceTracer getProvenanceTracer() {
        Factory<KeyValueStore> factoryForOrigins = getKeyValueStoreFactoryForOrigins();


        return isAnchored()
                ? getTracerOfOrigins(factoryForOrigins)
                : getTracerOfDescendants();
    }

    private Factory<KeyValueStore> getKeyValueStoreFactoryForOrigins() {
        return () -> getKeyValueStore(
                    new ValidatingKeyValueStreamContentAddressedFactory()
            );
    }

    public boolean isAnchored() {
        return !RefNodeConstants.BIODIVERSITY_DATASET_GRAPH.equals(getProvenanceAnchor());
    }

    protected ProvenanceTracer getTracerOfDescendants() {
        Factory<KeyValueStore> factory = () -> getKeyValueStore(
                new ValidatingKeyValueStreamHashTypeIRIFactory()
        );
        return getTracerOfDescendants(factory);
    }

    protected ProvenanceTracer getTracerOfDescendants(Factory<KeyValueStore>  keyValueStoreFactory) {
        HexaStoreImpl hexastore = new HexaStoreImpl(
                keyValueStoreFactory.create(),
                getHashType()
        );

        return new ProvenanceTracerByIndex(hexastore, getTracerOfOrigins(getKeyValueStoreFactoryForOrigins()));
    }

    protected ProvenanceTracer getTracerOfOrigins(Factory<KeyValueStore> keyValueStoreFactory) {
        return new ProvenanceTracerImpl(keyValueStoreFactory.create(), this);
    }


    public KeyToPath getKeyToPathLocal(URI baseURI) {
        initIfNeeded(baseURI);
        return keyToPathLocal;
    }

    public void setKeyToPathLocal(KeyToPath keyToPath) {
        this.keyToPathLocal = keyToPath;
    }

    private void initIfNeeded(URI baseURI) {
        if (keyToPathLocal == null) {
            if (depth == 2) {
                keyToPathLocal = new KeyTo3LevelPath(baseURI);
            } else if (depth == 0) {
                keyToPathLocal = new KeyTo1LevelPath(baseURI);
            } else {
                throw new IllegalArgumentException("only directory depths in {0,2} are supported, but found [" + depth + "]");
            }
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
