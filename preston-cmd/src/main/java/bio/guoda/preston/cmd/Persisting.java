package bio.guoda.preston.cmd;

import bio.guoda.preston.DerefProgressListener;
import bio.guoda.preston.store.DerefProgressLogger;
import bio.guoda.preston.store.KeyValueStoreConfig;
import bio.guoda.preston.stream.ContentStreamUtil;
import picocli.CommandLine;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class Persisting extends PersistingLocal {

    private static final String DISABLE_LOCAL_CONTENT_CACHE = "Disable local content cache";
    private static final String DISABLE_PROGRESS_MONITOR = "Disable progress monitor";
    private static final String DISABLE_ARCHIVE_DISCOVERY = "Do not index all remote data archive content; instead, request entire data archive and only retrieve the requested content.";

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

    @CommandLine.Option(
            names = {"--disable-archive-discovery"},
            description = DISABLE_ARCHIVE_DISCOVERY
    )
    private Boolean disableArchiveDiscovery = false;

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
                isSupportDiscoveryOfContentInArchives(),
                getProvenanceAnchor()
        );
    }

    private DerefProgressListener getProgressListener() {
        return disableProgress
                ? ContentStreamUtil.getNOOPDerefProgressListener()
                : new DerefProgressLogger();
    }

    protected void setSupportDiscoveryOfContentInArchives(boolean supportDiscoveryOfContentInArchives) {
        this.disableArchiveDiscovery = !supportDiscoveryOfContentInArchives;
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

    public boolean isSupportDiscoveryOfContentInArchives() {
        return !this.disableArchiveDiscovery;
    }

}
