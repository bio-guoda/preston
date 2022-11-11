package bio.guoda.preston.cmd;

import bio.guoda.preston.HashType;
import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.store.HexaStoreImpl;
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
import org.apache.commons.io.FileUtils;
import org.apache.commons.rdf.api.IRI;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;

import static bio.guoda.preston.RefNodeConstants.BIODIVERSITY_DATASET_GRAPH;

public class PersistingLocal extends Cmd {

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
            description = "Hash algorithm used to generate primary content identifiers. Supported values: ${COMPLETION-CANDIDATES}."
    )
    private HashType hashType = HashType.sha256;
    private KeyToPath keyToPathLocal = null;

    @CommandLine.Option(
            names = {"--provenanceRoot", "--provenanceAnchor", "--anchor", "-r"},
            description = "specify the provenance root/anchor of the command. By default, any available data graph will be traversed up to it's most recent additions. If the provenance root is set, only specified provenance signature and their origins are included in the scope."
    )
    private IRI provenanceAnchor = BIODIVERSITY_DATASET_GRAPH;

    public IRI getProvenanceAnchor() {
        return this.provenanceAnchor;
    }

    public void setProvenanceArchor(IRI provenanceAnchor) {
        this.provenanceAnchor = provenanceAnchor;
    }

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


    public ProvenanceTracer getProvenanceTracer() {
        return isAnchored()
                ? getTracerOfOrigins()
                : getTracerOfDescendants();
    }

    public boolean isAnchored() {
        return !RefNodeConstants.BIODIVERSITY_DATASET_GRAPH.equals(getProvenanceAnchor());
    }

    private ProvenanceTracer getTracerOfDescendants() {
        KeyValueStore keyValueStore = getKeyValueStore(
                new ValidatingKeyValueStreamHashTypeIRIFactory(getHashType())
        );
        return getTracerOfDescendants(keyValueStore);
    }

    protected ProvenanceTracer getTracerOfDescendants(KeyValueStore keyValueStore) {
        HexaStoreImpl hexastore = new HexaStoreImpl(
                keyValueStore,
                getHashType()
        );

        return new ProvenanceTracerByIndex(hexastore, getTracerOfOrigins());
    }

    private ProvenanceTracer getTracerOfOrigins() {
        KeyValueStore keyValueStore = getKeyValueStore(
                new ValidatingKeyValueStreamContentAddressedFactory(getHashType())
        );
        return new ProvenanceTracerImpl(keyValueStore, this);
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
