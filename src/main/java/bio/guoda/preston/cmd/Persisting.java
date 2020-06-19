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
import bio.guoda.preston.store.KeyValueStoreReadOnly;
import bio.guoda.preston.store.KeyValueStoreStickyFailover;
import bio.guoda.preston.store.KeyValueStoreWithDereferencing;
import bio.guoda.preston.store.KeyValueStoreWithFallback;
import bio.guoda.preston.store.KeyValueStreamFactory;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.URIConverter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
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
            Stream<Pair<URI,KeyToPath>> keyToPathStream =
                    getRemoteURIs().stream().flatMap(uri -> Stream.of(
                            Pair.of(uri, new KeyTo3LevelPath(uri)),
                            Pair.of(uri, new KeyTo1LevelPath(uri))
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
        return getRemoteURIs().stream().map(uri ->
                noLocalCache
                        ? this.remoteWithTarGz(uri)
                        : this.remoteWithTarGzCacheAll(uri, super.getKeyValueStore(new KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory())));
    }

    private KeyValueStoreReadOnly withStoreAt(URI baseURI, KeyToPath keyToPath) {
        return withStoreAt(keyToPath, getDerefStream(baseURI));
    }

    private KeyValueStoreReadOnly withStoreAt(KeyToPath keyToPath, Dereferencer<InputStream> dereferencer) {
        return new KeyValueStoreWithDereferencing(keyToPath, dereferencer);
    }

    protected void setSupportTarGzDiscovery(boolean supportTarGzDiscovery) {
        this.supportTarGzDiscovery = supportTarGzDiscovery;
    }


    public static Dereferencer<InputStream> getDerefStream(URI baseURI) {
        Dereferencer<InputStream> dereferencer;
        if (StringUtils.equalsAnyIgnoreCase(baseURI.getScheme(), "file")) {
            dereferencer = uri -> IOUtils.buffer(new FileInputStream(new File(URI.create(uri.getIRIString()))));
        } else {
            dereferencer = getDerefStreamHTTP(new DerefProgressLogger());
        }
        return dereferencer;
    }

    public static Dereferencer<InputStream> getDerefStreamHTTP(final DerefProgressListener listener) {
        return uri -> ResourcesHTTP.asInputStreamIgnore404(uri, listener);
    }

    private KeyValueStoreReadOnly remoteWithTarGz(URI baseURI) {
        return withStoreAt(new KeyTo3LevelTarGzPath(baseURI),
                new DereferencerContentAddressedTarGZ(getDerefStream(baseURI)));
    }

    private KeyValueStoreReadOnly remoteWithTarGzCacheAll(URI baseURI, KeyValueStore keyValueStore) {
        DereferencerContentAddressedTarGZ dereferencer = new DereferencerContentAddressedTarGZ(getDerefStream(baseURI), new BlobStoreAppendOnly(keyValueStore, false));
        return withStoreAt(new KeyTo3LevelTarGzPath(baseURI), dereferencer);
    }


    public void setRemoteURIs(List<URI> remoteURIs) {
        this.remoteURIs = remoteURIs;
    }
}
