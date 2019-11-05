package bio.guoda.preston.cmd;

import bio.guoda.preston.DerefProgressListener;
import bio.guoda.preston.Resources;
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
import bio.guoda.preston.store.KeyValueStoreRemoteHTTP;
import bio.guoda.preston.store.KeyValueStoreStickyFailover;
import bio.guoda.preston.store.KeyValueStoreWithFallback;
import bio.guoda.preston.store.KeyValueStreamFactory;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.URIConverter;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeConstants.ARCHIVE;

public class Persisting extends PersistingLocal {

    @Parameter(names = {"--remote", "--remotes"}, description = "remote repositories (e.g., https://deeplinker.bio/,https://example.org)", converter = URIConverter.class, validateWith = URIValidator.class)
    private List<URI> remoteURIs;

    @Parameter(names = {"--no-cache"}, description = "disable local content cache")
    private Boolean noLocalCache = false;

    private boolean supportTarGzDiscovery = true;

    private final IRI provenanceRoot = ARCHIVE;

    public IRI getProvenanceRoot() {
        return this.provenanceRoot;
    }

    protected List<URI> getRemoteURIs() {
        return remoteURIs;
    }

    protected boolean hasRemote() {
        return getRemoteURIs() != null && !getRemoteURIs().isEmpty();
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

    private Stream<KeyValueStoreRemoteHTTP> defaultRemotePathSupport(Stream<KeyToPath> keyToPathStream) {
        return keyToPathStream.map(this::remoteWith);
    }

    private List<KeyValueStoreReadOnly> includeTarGzSupport(Stream<KeyToPath> keyToPathStream) {
        return Stream.concat(
                defaultRemotePathSupport(keyToPathStream),
                tarGzRemotePathSupport()
        ).collect(Collectors.toList());
    }

    private Stream<KeyValueStoreRemoteHTTP> tarGzRemotePathSupport() {
        return getRemoteURIs().stream().map(uri ->
                noLocalCache
                        ? this.remoteWithTarGz(uri)
                        : this.remoteWithTarGzCacheAll(uri, super.getKeyValueStore(new KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory())));
    }

    private KeyValueStoreRemoteHTTP remoteWith(KeyToPath keyToPath) {
        return new KeyValueStoreRemoteHTTP(keyToPath, getDerefStream());
    }

    protected void setSupportTarGzDiscovery(boolean supportTarGzDiscovery) {
        this.supportTarGzDiscovery = supportTarGzDiscovery;
    }


    public static Dereferencer<InputStream> getDerefStream() {
        return getDerefStream(new DerefProgressLogger());
    }

    public static Dereferencer<InputStream> getDerefStream(final DerefProgressListener listener) {
        return new Dereferencer<InputStream>() {

            @Override
            public InputStream dereference(IRI uri) throws IOException {
                return Resources.asInputStreamIgnore404(uri, listener);
            }
        };
    }

    private KeyValueStoreRemoteHTTP remoteWithTarGz(URI baseURI) {
        return new KeyValueStoreRemoteHTTP(new KeyTo3LevelTarGzPath(baseURI),
                new DereferencerContentAddressedTarGZ(getDerefStream()));
    }

    private KeyValueStoreRemoteHTTP remoteWithTarGzCacheAll(URI baseURI, KeyValueStore keyValueStore) {
        return new KeyValueStoreRemoteHTTP(new KeyTo3LevelTarGzPath(baseURI),
                new DereferencerContentAddressedTarGZ(getDerefStream(), new BlobStoreAppendOnly(keyValueStore, false)));
    }


}
