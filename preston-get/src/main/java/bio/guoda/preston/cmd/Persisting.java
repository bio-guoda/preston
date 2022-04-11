package bio.guoda.preston.cmd;

import bio.guoda.preston.DerefProgressListener;
import bio.guoda.preston.ResourcesHTTP;
import bio.guoda.preston.store.AliasDereferencer;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.ContentHashDereferencer;
import bio.guoda.preston.store.Dereferencer;
import bio.guoda.preston.store.DereferencerContentAddressedTarGZ;
import bio.guoda.preston.store.KeyTo1LevelPath;
import bio.guoda.preston.store.KeyTo1LevelSoftwareHeritageAutoDetectPath;
import bio.guoda.preston.store.KeyTo1LevelSoftwareHeritagePath;
import bio.guoda.preston.store.KeyTo1LevelZenodoPath;
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
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Persisting extends PersistingLocal {

    @Parameter(names = {"--remote", "--remotes", "--include", "--repos", "--repositories"}, description = "included repositories dependency (e.g., https://deeplinker.bio/,https://example.org)", converter = URIConverter.class, validateWith = URIValidator.class)
    private List<URI> remotes = new ArrayList<>();

    @Parameter(names = {"--no-cache", "--disable-cache"}, description = "disable local content cache")
    private Boolean disableCache = false;

    @Parameter(names = {"--no-progress"}, description = "disable progress monitor")
    private Boolean disableProgress = false;

    private boolean supportTarGzDiscovery = true;

    protected List<URI> getRemotes() {
        return remotes;
    }

    protected boolean hasRemotes() {
        return getRemotes() != null && !getRemotes().isEmpty();
    }

    public void setDisableCache(Boolean disableCache) {
        this.disableCache = disableCache;
    }

    @Override
    protected KeyValueStore getKeyValueStore(KeyValueStreamFactory kvStreamFactory) {
        KeyValueStore store;
        if (hasRemotes()) {
            store = withRemoteSupport(kvStreamFactory);
        } else {
            store = super.getKeyValueStore(kvStreamFactory);
        }
        return store;
    }

    private KeyValueStore withRemoteSupport(KeyValueStreamFactory kvStreamFactory) {
        KeyValueStore store;
        Stream<Pair<URI, KeyToPath>> keyToPathStream =
                getRemotes()
                        .stream()
                        .flatMap(uri -> Stream.of(
                                Pair.of(uri, new KeyTo3LevelPath(uri, getHashType())),
                                Pair.of(uri, new KeyTo1LevelPath(uri, getHashType())),
                                Pair.of(uri, new KeyTo1LevelSoftwareHeritagePath(uri)),
                                Pair.of(uri, new KeyTo1LevelSoftwareHeritageAutoDetectPath(uri)),
                                Pair.of(uri, new KeyTo1LevelZenodoPath(uri, getDerefStream(uri, getProgressListener())))
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
        return store;
    }

    private Stream<KeyValueStoreReadOnly> defaultRemotePathSupport(Stream<Pair<URI, KeyToPath>> keyToPathStream) {
        return keyToPathStream.map(x -> this.withStoreAt(x.getKey(), x.getValue()));
    }

    private List<KeyValueStoreReadOnly> includeTarGzSupport(Stream<Pair<URI, KeyToPath>> keyToPathStream) {
        return Stream.concat(
                defaultRemotePathSupport(keyToPathStream),
                tarGzRemotePathSupport()
        ).collect(Collectors.toList());
    }

    private Stream<KeyValueStoreReadOnly> tarGzRemotePathSupport() {
        return getRemotes().stream().map(uri ->
                disableCache
                        ? this.remoteWithTarGz(uri)
                        : this.remoteWithTarGzCacheAll(uri, super.getKeyValueStore(new KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory(getHashType()))));
    }

    private KeyValueStoreReadOnly withStoreAt(URI baseURI, KeyToPath keyToPath) {
        return withStoreAt(keyToPath, getDerefStream(baseURI, getProgressListener()));
    }

    private DerefProgressListener getProgressListener() {
        return disableProgress
                ? ContentStreamUtil.getNOOPDerefProgressListener()
                : new DerefProgressLogger();
    }

    private KeyValueStoreReadOnly withStoreAt(KeyToPath keyToPath, Dereferencer<InputStream> dereferencer) {
        return new KeyValueStoreWithDereferencing(keyToPath, dereferencer);
    }

    protected void setSupportTarGzDiscovery(boolean supportTarGzDiscovery) {
        this.supportTarGzDiscovery = supportTarGzDiscovery;
    }


    private static Dereferencer<InputStream> getDerefStream(URI baseURI, DerefProgressListener listener) {
        Dereferencer<InputStream> dereferencer;
        if (StringUtils.equalsAnyIgnoreCase(baseURI.getScheme(), "file")) {
            dereferencer = getInputStreamDereferencerFile(listener);
        } else {
            dereferencer = getDerefStreamHTTP(listener);
        }
        return dereferencer;
    }

    private static Dereferencer<InputStream> getInputStreamDereferencerFile(DerefProgressListener listener) {
        return uri -> {
            URI uri1 = URI.create(uri.getIRIString());
            File file = new File(uri1);
            return file.exists()
                    ? getInputStreamForFile(uri, file, listener)
                    : null;
        };
    }

    private static InputStream getInputStreamForFile(IRI uri, File file, DerefProgressListener listener) throws FileNotFoundException {
        return ContentStreamUtil.getInputStreamWithProgressLogger(uri, listener, file.length(), IOUtils.buffer(new FileInputStream(file)));
    }

    public static Dereferencer<InputStream> getDerefStreamHTTP(final DerefProgressListener listener) {
        return uri -> ResourcesHTTP.asInputStreamIgnore404(uri, listener);
    }

    private KeyValueStoreReadOnly remoteWithTarGz(URI baseURI) {
        return withStoreAt(new KeyTo3LevelTarGzPath(baseURI, getHashType()),
                new DereferencerContentAddressedTarGZ(getDerefStream(baseURI, getProgressListener())));
    }

    private KeyValueStoreReadOnly remoteWithTarGzCacheAll(URI baseURI, KeyValueStore keyValueStore) {
        DereferencerContentAddressedTarGZ dereferencer =
                new DereferencerContentAddressedTarGZ(getDerefStream(baseURI,
                        getProgressListener()),
                        new BlobStoreAppendOnly(keyValueStore, false, getHashType()));

        return withStoreAt(new KeyTo3LevelTarGzPath(baseURI, getHashType()), dereferencer);
    }

    protected BlobStoreReadOnly resolvingBlobStore(Dereferencer<InputStream> blobStore) {
        return new AliasDereferencer(
                new ContentHashDereferencer(blobStore),
                this
        );
    }


    public void setRemotes(List<URI> remotes) {
        this.remotes = remotes;
    }

    public Boolean getDisableCache() {
        return disableCache;
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
