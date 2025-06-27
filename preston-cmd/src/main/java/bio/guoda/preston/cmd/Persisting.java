package bio.guoda.preston.cmd;

import bio.guoda.preston.DerefProgressListener;
import bio.guoda.preston.store.DerefProgressLogger;
import bio.guoda.preston.store.KeyValueStore;
import bio.guoda.preston.store.KeyValueStoreConfig;
import bio.guoda.preston.store.KeyValueStoreFactoryImpl;
import bio.guoda.preston.store.ValidatingKeyValueStreamFactory;
import bio.guoda.preston.stream.ContentStreamUtil;
import picocli.CommandLine;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class Persisting extends PersistingLocal {

    private static final String DISABLE_LOCAL_CONTENT_CACHE = "Disable local content cache";
    private static final String DISABLE_PROGRESS_MONITOR = "Disable progress monitor";

    @CommandLine.Option(
            names = {"--remote", "--remotes", "--include", "--repos", "--repositories"},
            split = ",",
            description = "Included repository dependencies (e.g., https://linker.bio/,https://softwareheritage.org,https://wikimedia.org,https://dataone.org,https://zenodo.org)"
    )

    private List<URI> remotes = new ArrayList<>();

    @CommandLine.Option(
            names = {"--no-cache", "--disable-cache"},
            defaultValue = "false",
            description = DISABLE_LOCAL_CONTENT_CACHE
    )
    private Boolean disableCache = false;

    @CommandLine.Option(
            names = {"--no-progress"},
            description = DISABLE_PROGRESS_MONITOR
    )
    private Boolean disableProgress = false;

    private boolean supportTarGzDiscovery = true;

    public List<URI> getRemotes() {
        return remotes;
    }

    public void setCacheEnabled(Boolean enableCache) {
        this.disableCache = !enableCache;
    }

    public boolean isCacheEnabled() {
        return !this.disableCache;
    }


    @Override
    protected KeyValueStoreConfig getKeyValueStoreConfig() {
        return new KeyValueStoreConfig(
                    new File(getDataDir()),
                    new File(getTmpDir()),
                    getDepth(),
                    isCacheEnabled(),
                    getRemotes(),
                    getHashType(),
                    getProgressListener(),
                    isSupportTarGzDiscovery()
            );
    }

    private DerefProgressListener getProgressListener() {
        return disableProgress
                ? ContentStreamUtil.getNOOPDerefProgressListener()
                : new DerefProgressLogger();
    }

    protected void setSupportTarGzDiscovery(boolean supportTarGzDiscovery) {
        this.supportTarGzDiscovery = supportTarGzDiscovery;
    }


    public void setRemotes(List<URI> remotes) {
        this.remotes = remotes;
    }


    public Boolean getDisableProgress() {
        return disableProgress;
    }

    public void setDisableProgress(Boolean disableProgress) {
        this.disableProgress = disableProgress;
    }

    public boolean isSupportTarGzDiscovery() {
        return supportTarGzDiscovery;
    }

}
