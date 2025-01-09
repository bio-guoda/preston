package bio.guoda.preston.cmd;

import bio.guoda.preston.HashType;
import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.store.HexaStoreImpl;
import bio.guoda.preston.store.KeyValueStore;
import bio.guoda.preston.store.KeyValueStoreConfig;
import bio.guoda.preston.store.KeyValueStoreFactoryImpl;
import bio.guoda.preston.store.KeyValueStoreUtil;
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

public class PersistingLocal extends CmdWithProvenance {

    @CommandLine.Option(
            names = {"--data-dir"},
            defaultValue = "data",
            description = "Location of local content cache"
    )
    private String dataDir = "data";

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
    private String tmpDir = "tmp";

    @CommandLine.Option(
            names = {"--hash-algorithm", "--algo", "-a"},
            description = "Hash algorithm used to generate primary content identifiers. Supported values: ${COMPLETION-CANDIDATES}."
    )
    private HashType hashType = HashType.sha256;

    static File mkdir(String data1) {
        File data = new File(data1);
        try {
            FileUtils.forceMkdir(data);
        } catch (IOException e) {
            //
        }
        return data;
    }

    public ProvenanceTracer getProvenanceTracer() {
        Factory<KeyValueStore> factoryForOrigins = getKeyValueStoreFactoryForOrigins();

        return isAnchored()
                ? getTracerOfOrigins(factoryForOrigins)
                : getTracerOfDescendants();
    }

    protected KeyValueStoreConfig getKeyValueStoreConfig() {
        return new KeyValueStoreConfig(new File(getDataDir()), new File(getTmpDir()), getDepth());
    }

    protected KeyValueStore getKeyValueStore(ValidatingKeyValueStreamFactory kvStreamFactory) {
        return new KeyValueStoreFactoryImpl(getKeyValueStoreConfig()).getKeyValueStore(kvStreamFactory);

    }


    private Factory<KeyValueStore> getKeyValueStoreFactoryForOrigins() {
        return () ->
                getKeyValueStore(
                        new ValidatingKeyValueStreamContentAddressedFactory()
                );
    }

    public boolean isAnchored() {
        return !RefNodeConstants.BIODIVERSITY_DATASET_GRAPH.equals(getProvenanceAnchor());
    }

    protected ProvenanceTracer getTracerOfDescendants() {
        Factory<KeyValueStore> factory = () ->
                getKeyValueStore(
                        new ValidatingKeyValueStreamHashTypeIRIFactory()
                );
        return getTracerOfDescendants(factory);
    }

    protected ProvenanceTracer getTracerOfDescendants(Factory<KeyValueStore> keyValueStoreFactory) {
        HexaStoreImpl hexastore = new HexaStoreImpl(
                keyValueStoreFactory.create(),
                getHashType()
        );

        return new ProvenanceTracerByIndex(hexastore, getTracerOfOrigins(getKeyValueStoreFactoryForOrigins()));
    }

    protected ProvenanceTracer getTracerOfOrigins(Factory<KeyValueStore> keyValueStoreFactory) {
        return new ProvenanceTracerImpl(keyValueStoreFactory.create(), this);
    }

    public int getDepth() {
        return depth;
    }

    public void setDepth(int depth) {
        this.depth = depth;
    }

    public String getDataDir() {
        mkdir(dataDir);
        return dataDir;
    }

    public String getTmpDir() {
        mkdir(tmpDir);
        return tmpDir;
    }

    public void setDataDir(String dataDir) {
        this.dataDir = dataDir;
    }

    public void setTmpDir(String tmpDir) {
        this.tmpDir = tmpDir;
    }

    public HashType getHashType() {
        return hashType;
    }

    public void setHashType(HashType hashType) {
        this.hashType = hashType;
    }

}
