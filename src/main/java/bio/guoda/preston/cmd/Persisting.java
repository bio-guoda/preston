package bio.guoda.preston.cmd;

import bio.guoda.preston.DerefProgressListener;
import bio.guoda.preston.ResourcesHTTP;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.Dereferencer;
import bio.guoda.preston.store.DereferencerContentAddressedTarGZ;
import bio.guoda.preston.store.KeyTo1LevelPath;
import bio.guoda.preston.store.KeyTo3LevelPath;
import bio.guoda.preston.store.KeyTo3LevelTarGzPath;
import bio.guoda.preston.store.KeyToPath;
import bio.guoda.preston.store.KeyValueStore;
import bio.guoda.preston.store.KeyValueStoreCopying;
import bio.guoda.preston.store.KeyValueStoreLocalFileSystem;
import bio.guoda.preston.store.KeyValueStoreLocalFileSystemReadOnly;
import bio.guoda.preston.store.KeyValueStoreProtocolAware;
import bio.guoda.preston.store.KeyValueStoreReadOnly;
import bio.guoda.preston.store.KeyValueStoreWithDereferencing;
import bio.guoda.preston.store.KeyValueStoreStickyFailover;
import bio.guoda.preston.store.KeyValueStoreWithFallback;
import bio.guoda.preston.store.KeyValueStreamFactory;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.URIConverter;
import org.apache.commons.rdf.api.IRI;

import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeConstants.BIODIVERSITY_DATASET_GRAPH;

public class Persisting extends PersistingLocal {

    @Parameter(names = {"--remote", "--remotes", "--include", "--depends-on", "--use"}, description = "included repositories dependency (e.g., https://deeplinker.bio/,https://example.org)", converter = URIConverter.class, validateWith = URIValidator.class)
    private List<URI> remoteURIs;

    @Parameter(names = {"--no-cache"}, description = "disable local content cache")
    private Boolean noLocalCache = false;

    private boolean supportTarGzDiscovery = true;

    private final IRI provenanceRoot = BIODIVERSITY_DATASET_GRAPH;

    public IRI getProvenanceRoot() {
        return this.provenanceRoot;
    }

    protected List<URI> getRemoteURIs() {
        return remoteURIs;
    }

    protected boolean hasRemote() {
        return getRemoteURIs() != null && !getRemoteURIs().isEmpty();
    }

    protected void setNoLocalCache(Boolean noLocalCache) {
        this.noLocalCache = noLocalCache;
    }

    @Override
    protected KeyValueStore getKeyValueStore(KeyValueStreamFactory kvStreamFactory) {
        KeyValueStore store;
        if (hasRemote()) {
            Stream<KeyToPath> keyToPathStream =
                    getRemoteURIs().stream().flatMap(uri -> Stream.of(
                            new KeyTo3LevelPath(uri),
                            new KeyTo1LevelPath(uri)
                    ));

            List<KeyValueStoreReadOnly> keyValueStoreRemotes =
                    supportTarGzDiscovery
                            ? includeTarGzSupport(keyToPathStream)
                            : defaultRemotePathSupport(keyToPathStream).collect(Collectors.toList());

            KeyValueStoreStickyFailover failover = new KeyValueStoreStickyFailover(keyValueStoreRemotes);

            if (noLocalCache) {
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

    private Stream<KeyValueStoreReadOnly> defaultRemotePathSupport(Stream<KeyToPath> keyToPathStream) {
        return keyToPathStream.map(this::withStoreAt);
    }

    private List<KeyValueStoreReadOnly> includeTarGzSupport(Stream<KeyToPath> keyToPathStream) {
        return Stream.concat(
                defaultRemotePathSupport(keyToPathStream),
                tarGzRemotePathSupport()
        ).collect(Collectors.toList());
    }

    private Stream<KeyValueStoreWithDereferencing> tarGzRemotePathSupport() {
        return getRemoteURIs().stream().map(uri ->
                noLocalCache
                        ? this.remoteWithTarGz(uri)
                        : this.remoteWithTarGzCacheAll(uri, super.getKeyValueStore(new KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory())));
    }

    private KeyValueStoreReadOnly withStoreAt(KeyToPath keyToPath) {
        return new KeyValueStoreWithDereferencing(keyToPath, getDerefStream());
//        return new KeyValueStoreProtocolAware(new TreeMap<String, KeyValueStoreReadOnly>() {{
//            put("http", new KeyValueStoreWithDereferencing(keyToPath, getDerefStream()));
//            put("file:", new KeyValueStoreLocalFileSystemReadOnly(keyToPath));
//        }});
    }

    protected void setSupportTarGzDiscovery(boolean supportTarGzDiscovery) {
        this.supportTarGzDiscovery = supportTarGzDiscovery;
    }


    public static Dereferencer<InputStream> getDerefStream() {
        return getDerefStream(new DerefProgressLogger());
    }

    public static Dereferencer<InputStream> getDerefStream(final DerefProgressListener listener) {
        return uri -> ResourcesHTTP.asInputStreamIgnore404(uri, listener);
    }

    private KeyValueStoreWithDereferencing remoteWithTarGz(URI baseURI) {
        return new KeyValueStoreWithDereferencing(new KeyTo3LevelTarGzPath(baseURI),
                new DereferencerContentAddressedTarGZ(getDerefStream()));
    }

    private KeyValueStoreWithDereferencing remoteWithTarGzCacheAll(URI baseURI, KeyValueStore keyValueStore) {
        return new KeyValueStoreWithDereferencing(new KeyTo3LevelTarGzPath(baseURI),
                new DereferencerContentAddressedTarGZ(getDerefStream(), new BlobStoreAppendOnly(keyValueStore, false)));
    }


    public void setRemoteURIs(List<URI> remoteURIs) {
        this.remoteURIs = remoteURIs;
    }
}
