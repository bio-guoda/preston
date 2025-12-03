package bio.guoda.preston.store;

import bio.guoda.preston.DerefProgressListener;
import bio.guoda.preston.HashType;
import bio.guoda.preston.stream.ContentStreamUtil;
import org.apache.commons.rdf.api.IRI;

import java.io.File;
import java.net.URI;
import java.util.Collections;
import java.util.List;

public class KeyValueStoreConfig {
    private final File dataDir;
    private final File tmpDir;
    private final int directoryDepth;
    private final boolean cacheEnabled;
    private final List<URI> remotes;
    private final HashType hashType;
    private final DerefProgressListener progressListener;
    private final boolean supportDiscoveryOfContentInArchives;
    private final IRI anchor;

    public KeyValueStoreConfig(File dataDir,
                               File tmpDir,
                               int directoryDepth,
                               boolean cacheEnabled,
                               List<URI> remotes,
                               HashType hashType,
                               DerefProgressListener progressListener,
                               boolean supportDiscoveryOfContentInArchives) {
        this(dataDir,
                tmpDir,
                directoryDepth,
                cacheEnabled,
                remotes,
                hashType,
                progressListener,
                supportDiscoveryOfContentInArchives,
                null
        );
    }

    public KeyValueStoreConfig(File dataDir, File tmpDir, int directoryDepth) {
        this(dataDir,
                tmpDir,
                directoryDepth,
                false,
                Collections.emptyList(),
                HashType.sha256,
                ContentStreamUtil.NOOP_DEREF_PROGRESS_LISTENER,
                false
        );
    }

    public KeyValueStoreConfig(File dataDir, File tmpDir, int directoryDepth, boolean cacheEnabled, List<URI> remotes, HashType hashType, DerefProgressListener progressListener, boolean supportDiscoveryOfContentInArchives, IRI provenanceAnchor) {
        this.dataDir = dataDir;
        this.tmpDir = tmpDir;
        this.directoryDepth = directoryDepth;
        this.cacheEnabled = cacheEnabled;
        this.remotes = remotes;
        this.hashType = hashType;
        this.progressListener = progressListener;
        this.supportDiscoveryOfContentInArchives = supportDiscoveryOfContentInArchives;
        this.anchor = provenanceAnchor;
    }

    public File getDataDir() {
        return dataDir;
    }

    public File getTmpDir() {
        return tmpDir;
    }

    public int getDirectoryDepth() {
        return directoryDepth;
    }

    public boolean isCacheEnabled() {
        return cacheEnabled;
    }

    public List<URI> getRemotes() {
        return remotes;
    }

    public HashType getHashType() {
        return hashType;
    }

    public DerefProgressListener getProgressListener() {
        return progressListener;
    }

    public boolean isSupportDiscoveryOfContentInArchives() {
        return supportDiscoveryOfContentInArchives;
    }

    public IRI getAnchor() {
        return anchor;
    }
}
