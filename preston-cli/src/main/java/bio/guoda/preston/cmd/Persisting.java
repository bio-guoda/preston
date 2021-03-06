package bio.guoda.preston.cmd;

import bio.guoda.preston.DerefProgressListener;
import bio.guoda.preston.ResourcesHTTP;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.Dereferencer;
import bio.guoda.preston.store.DereferencerContentAddressedTarGZ;
import bio.guoda.preston.store.KeyTo1LevelPath;
import bio.guoda.preston.store.KeyTo1LevelSoftwareHeritageAutoDetectPath;
import bio.guoda.preston.store.KeyTo1LevelSoftwareHeritagePath;
import bio.guoda.preston.store.KeyTo3LevelPath;
import bio.guoda.preston.store.KeyTo3LevelTarGzPath;
import bio.guoda.preston.store.KeyToPath;
import bio.guoda.preston.store.KeyValueStore;
import bio.guoda.preston.store.KeyValueStoreCopying;
import bio.guoda.preston.store.KeyValueStoreLocalFileSystem;
import bio.guoda.preston.store.KeyValueStoreReadOnly;
import bio.guoda.preston.store.KeyValueStoreStickyFailover;
import bio.guoda.preston.store.KeyValueStoreWithDereferencing;
import bio.guoda.preston.store.KeyValueStoreWithFallback;
import bio.guoda.preston.store.KeyValueStreamFactory;
import bio.guoda.preston.stream.ContentStreamUtil;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.URIConverter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeConstants.BIODIVERSITY_DATASET_GRAPH;

public class Persisting extends PersistingLocal {

    @Parameter(names = {"--remote", "--remotes", "--include", "--repos", "--repositories"}, description = "included repositories dependency (e.g., https://deeplinker.bio/,https://example.org)", converter = URIConverter.class, validateWith = URIValidator.class)
    private List<URI> repositoryURIs;

    @Parameter(names = {"--no-cache", "--disable-cache"}, description = "disable local content cache")
    private Boolean disableCache = false;

    @Parameter(names = {"--no-progress"}, description = "disable progress monitor")
    private Boolean disableProgress = false;

    private boolean supportTarGzDiscovery = true;

    private final IRI provenanceRoot = BIODIVERSITY_DATASET_GRAPH;

    public IRI getProvenanceRoot() {
        return this.provenanceRoot;
    }

    protected List<URI> getRepositoryURIs() {
        return repositoryURIs;
    }

    protected boolean hasRepositoryDependencies() {
        return getRepositoryURIs() != null && !getRepositoryURIs().isEmpty();
    }

    protected void setDisableCache(Boolean disableCache) {
        this.disableCache = disableCache;
    }

    @Override
    protected KeyValueStore getKeyValueStore(KeyValueStreamFactory kvStreamFactory) {
        KeyValueStore store;
        if (hasRepositoryDependencies()) {
            Stream<Pair<URI,KeyToPath>> keyToPathStream =
                    getRepositoryURIs().stream().flatMap(uri -> Stream.of(
                            Pair.of(uri, new KeyTo3LevelPath(uri)),
                            Pair.of(uri, new KeyTo1LevelPath(uri)),
                            Pair.of(uri, new KeyTo1LevelSoftwareHeritagePath(uri)),
                            Pair.of(uri, new KeyTo1LevelSoftwareHeritageAutoDetectPath(uri))
                    ));

            List<KeyValueStoreReadOnly> keyValueStoreRemotes =
                    supportTarGzDiscovery
                            ? includeTarGzSupport(keyToPathStream)
                            : defaultRemotePathSupport(keyToPathStream).collect(Collectors.toList());

            KeyValueStoreStickyFailover failover = new KeyValueStoreStickyFailover(keyValueStoreRemotes);

            if (disableCache) {
                store = new KeyValueStoreWithFallback(
                        super.getKeyValueStore(kvStreamFactory),
                        failover);
            } else {
                store = new KeyValueStoreCopying(
                        failover,
                        super.getKeyValueStore(kvStreamFactory));
            }
        } else {
            store = super.getKeyValueStore(kvStreamFactory);
        }
        return store;
    }

    private Stream<KeyValueStoreReadOnly> defaultRemotePathSupport(Stream<Pair<URI, KeyToPath>> keyToPathStream) {
        return keyToPathStream.map(x -> this.withStoreAt(x.getKey(), x.getValue()));
    }

    private List<KeyValueStoreReadOnly> includeTarGzSupport(Stream<Pair<URI,KeyToPath>> keyToPathStream) {
        return Stream.concat(
                defaultRemotePathSupport(keyToPathStream),
                tarGzRemotePathSupport()
        ).collect(Collectors.toList());
    }

    private Stream<KeyValueStoreReadOnly> tarGzRemotePathSupport() {
        return getRepositoryURIs().stream().map(uri ->
                disableCache
                        ? this.remoteWithTarGz(uri)
                        : this.remoteWithTarGzCacheAll(uri, super.getKeyValueStore(new KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory())));
    }

    private KeyValueStoreReadOnly withStoreAt(URI baseURI, KeyToPath keyToPath) {
        return withStoreAt(keyToPath, getDerefStream(baseURI, getProgressListener()));
    }

    private DerefProgressListener getProgressListener() {
        return disableProgress
                ? ContentStreamUtil.getNullDerefProgressListener()
                : new DerefProgressLogger();
    }

    private KeyValueStoreReadOnly withStoreAt(KeyToPath keyToPath, Dereferencer<InputStream> dereferencer) {
        return new KeyValueStoreWithDereferencing(keyToPath, dereferencer);
    }

    protected void setSupportTarGzDiscovery(boolean supportTarGzDiscovery) {
        this.supportTarGzDiscovery = supportTarGzDiscovery;
    }


    public static Dereferencer<InputStream> getDerefStream(URI baseURI, DerefProgressListener listener) {
        Dereferencer<InputStream> dereferencer;
        if (StringUtils.equalsAnyIgnoreCase(baseURI.getScheme(), "file")) {
            dereferencer = getInputStreamDereferencerFile(listener);
        } else {
            dereferencer = getDerefStreamHTTP(listener);
        }
        return dereferencer;
    }

    public static Dereferencer<InputStream> getInputStreamDereferencerFile(DerefProgressListener listener) {
        return uri -> {
            URI uri1 = URI.create(uri.getIRIString());
            File file = new File(uri1);
            return file.exists()
                    ? getInputStreamForFile(uri, file, listener)
                    : null;
        };
    }

    public static InputStream getInputStreamForFile(IRI uri, File file, DerefProgressListener listener) throws FileNotFoundException {
        return ContentStreamUtil.getInputStreamWithProgressLogger(uri, listener, file.length(), IOUtils.buffer(new FileInputStream(file)));
    }

    public static Dereferencer<InputStream> getDerefStreamHTTP(final DerefProgressListener listener) {
        return uri -> ResourcesHTTP.asInputStreamIgnore404(uri, listener);
    }

    private KeyValueStoreReadOnly remoteWithTarGz(URI baseURI) {
        return withStoreAt(new KeyTo3LevelTarGzPath(baseURI),
                new DereferencerContentAddressedTarGZ(getDerefStream(baseURI, getProgressListener())));
    }

    private KeyValueStoreReadOnly remoteWithTarGzCacheAll(URI baseURI, KeyValueStore keyValueStore) {
        DereferencerContentAddressedTarGZ dereferencer = new DereferencerContentAddressedTarGZ(getDerefStream(baseURI, getProgressListener()), new BlobStoreAppendOnly(keyValueStore, false));
        return withStoreAt(new KeyTo3LevelTarGzPath(baseURI), dereferencer);
    }


    public void setRepositoryURIs(List<URI> repositoryURIs) {
        this.repositoryURIs = repositoryURIs;
    }
}
