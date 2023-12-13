package bio.guoda.preston.cmd;

import bio.guoda.preston.DerefProgressListener;
import bio.guoda.preston.ResourcesHTTP;
import bio.guoda.preston.store.AliasDereferencer;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.ContentHashDereferencer;
import bio.guoda.preston.store.DerefProgressLogger;
import bio.guoda.preston.store.Dereferencer;
import bio.guoda.preston.store.DereferencerContentAddressedTarGZ;
import bio.guoda.preston.store.KeyTo1LevelDataOnePath;
import bio.guoda.preston.store.KeyTo1LevelDataVersePath;
import bio.guoda.preston.store.KeyTo1LevelOCIPath;
import bio.guoda.preston.store.KeyTo1LevelPath;
import bio.guoda.preston.store.KeyTo1LevelSoftwareHeritageAutoDetectPath;
import bio.guoda.preston.store.KeyTo1LevelSoftwareHeritagePath;
import bio.guoda.preston.store.KeyTo1LevelWikiMediaCommonsPath;
import bio.guoda.preston.store.KeyTo1LevelZenodoBucket;
import bio.guoda.preston.store.KeyTo1LevelZenodoPath;
import bio.guoda.preston.store.KeyTo3LevelPath;
import bio.guoda.preston.store.KeyTo3LevelTarGzPath;
import bio.guoda.preston.store.KeyToHashURI;
import bio.guoda.preston.store.KeyToPath;
import bio.guoda.preston.store.KeyValueStore;
import bio.guoda.preston.store.KeyValueStoreCopying;
import bio.guoda.preston.store.KeyValueStoreLocalFileSystem;
import bio.guoda.preston.store.KeyValueStoreReadOnly;
import bio.guoda.preston.store.KeyValueStoreStickyFailover;
import bio.guoda.preston.store.KeyValueStoreWithDereferencing;
import bio.guoda.preston.store.KeyValueStoreWithFallback;
import bio.guoda.preston.store.KeyValueStoreWithValidation;
import bio.guoda.preston.store.ValidatingKeyValueStreamContentAddressedFactory;
import bio.guoda.preston.store.ValidatingKeyValueStreamFactory;
import bio.guoda.preston.stream.ContentStreamUtil;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import picocli.CommandLine;

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
            names = {"--no-progress", "--no-process"},
            description = DISABLE_PROGRESS_MONITOR
    )
    private Boolean disableProgress = false;

    private boolean supportTarGzDiscovery = true;

    public List<URI> getRemotes() {
        return remotes;
    }

    public boolean hasRemotes() {
        return getRemotes() != null && !getRemotes().isEmpty();
    }

    public void setCacheEnabled(Boolean enableCache) {
        this.disableCache = !enableCache;
    }

    public boolean isCacheEnabled() {
        return !this.disableCache;
    }


    @Override
    protected KeyValueStore getKeyValueStore(ValidatingKeyValueStreamFactory kvStreamFactory) {
        KeyValueStore store;
        if (hasRemotes()) {
            store = withRemoteSupport(kvStreamFactory);
        } else {
            store = super.getKeyValueStore(kvStreamFactory);
        }
        return store;
    }

    private KeyValueStore withRemoteSupport(ValidatingKeyValueStreamFactory kvStreamFactory) {
        KeyValueStore store;
        Stream<Pair<URI, KeyToPath>> keyToPathStream =
                getRemotes()
                        .stream()
                        .flatMap(uri -> Stream.of(
                                Pair.of(uri, new KeyToHashURI(uri)),
                                Pair.of(uri, new KeyTo3LevelPath(uri)),
                                Pair.of(uri, new KeyTo1LevelPath(uri)),
                                Pair.of(uri, new KeyTo1LevelSoftwareHeritagePath(uri)),
                                Pair.of(uri, new KeyTo1LevelSoftwareHeritageAutoDetectPath(uri)),
                                Pair.of(uri, new KeyTo1LevelZenodoBucket(new KeyTo1LevelZenodoPath(uri, getDerefStream(uri, getProgressListener())))),
                                Pair.of(uri, new KeyTo1LevelZenodoBucket(new KeyTo1LevelZenodoPath(uri, getDerefStream(uri, getProgressListener()), KeyTo1LevelZenodoPath.ZENODO_API_PREFIX_2023_10_13, KeyTo1LevelZenodoPath.ZENODO_API_SUFFIX_2023_10_13))),
                                Pair.of(uri, new KeyTo1LevelDataOnePath(uri, getDerefStream(uri, getProgressListener()))),
                                Pair.of(uri, new KeyTo1LevelOCIPath(uri)),
                                Pair.of(uri, new KeyTo1LevelWikiMediaCommonsPath(uri, getDerefStream(uri, getProgressListener()))),
                                Pair.of(uri, new KeyTo1LevelDataVersePath(uri, getDerefStream(uri, getProgressListener())))
                        ));

        List<KeyValueStoreReadOnly> keyValueStoreRemotes =
                supportTarGzDiscovery
                        ? includeTarGzSupport(keyToPathStream)
                        : defaultRemotePathSupport(keyToPathStream).collect(Collectors.toList());


        if (isCacheEnabled()) {
            store = new KeyValueStoreCopying(
                    createStickyFailoverWithValidatedCache(kvStreamFactory, keyValueStoreRemotes),
                    super.getKeyValueStore(kvStreamFactory)
            );
        } else {
            KeyValueStoreStickyFailover failover = new KeyValueStoreStickyFailover(keyValueStoreRemotes);
            store = new KeyValueStoreWithFallback(
                    super.getKeyValueStore(kvStreamFactory),
                    failover
            );
        }
        return store;
    }

    private KeyValueStoreStickyFailover createStickyFailoverWithValidatedCache(
            ValidatingKeyValueStreamFactory kvStreamFactory,
            List<KeyValueStoreReadOnly> remotes
    ) {
        File stagingDir = getTmpDir();

        List<KeyValueStoreReadOnly> validatedRemotes = remotes
                .stream()
                .map(remote -> {
                    KeyValueStoreLocalFileSystem staging = new KeyValueStoreLocalFileSystem(
                            getTmpDir(),
                            new KeyTo1LevelPath(stagingDir.toURI()),
                            kvStreamFactory
                    );
                    KeyValueStore validated = super.getKeyValueStore(kvStreamFactory);

                    return new KeyValueStoreWithValidation(
                            kvStreamFactory,
                            staging,
                            validated,
                            remote
                    );
                })
                .collect(Collectors.toList());


        return new KeyValueStoreStickyFailover(validatedRemotes);
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
                isCacheEnabled()
                        ? this.remoteWithTarGzCacheAll(uri, super.getKeyValueStore(new ValidatingKeyValueStreamContentAddressedFactory()))
                        : this.remoteWithTarGz(uri));
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
        return uri -> ResourcesHTTP.asInputStreamIgnore40x50x(uri, listener);
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
        return resolvingBlobStore(blobStore, this);
    }

    public static BlobStoreReadOnly resolvingBlobStore(Dereferencer<InputStream> blobStore, Persisting persisting) {
        return new AliasDereferencer(
                new ContentHashDereferencer(blobStore),
                persisting,
                persisting.getProvenanceTracer()
        );
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
