package bio.guoda.preston.store;

import bio.guoda.preston.DerefProgressListener;
import bio.guoda.preston.HashType;
import bio.guoda.preston.ResourcesHTTP;
import bio.guoda.preston.stream.ContentStreamUtil;
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

public class KeyValueStoreUtil {

    public static KeyValueStore getKeyValueStore(
            final ValidatingKeyValueStreamFactory validatingKeyValueStreamFactory,
            final File dataDir,
            final File tmpDir,
            int directoryDepth) {

        KeyValueStoreConfig config = new KeyValueStoreConfig(
                dataDir,
                tmpDir,
                directoryDepth
        );

        return new KeyValueStoreFactoryFallBack(
                config
        ).getKeyValueStore(validatingKeyValueStreamFactory);
    }


    private static KeyValueStoreStickyFailover createStickyFailoverWithValidatedCache(
            ValidatingKeyValueStreamFactory kvStreamFactory,
            List<KeyValueStoreReadOnly> remotes,
            File stagingDir,
            KeyValueStore keyStore
    ) {

        List<KeyValueStoreReadOnly> validatedRemotes = remotes
                .stream()
                .map(remote -> {
                    KeyValueStoreLocalFileSystem staging = new KeyValueStoreLocalFileSystem(
                            stagingDir,
                            new KeyTo1LevelPath(stagingDir.toURI()),
                            kvStreamFactory
                    );

                    return new KeyValueStoreWithValidation(
                            kvStreamFactory,
                            staging,
                            keyStore,
                            remote
                    );
                })
                .collect(Collectors.toList());


        return new KeyValueStoreStickyFailover(validatedRemotes);
    }


    static KeyValueStore withRemoteSupport(ValidatingKeyValueStreamFactory kvStreamFactory,
                                           KeyValueStore keyValueStore,
                                           KeyValueStoreConfig config) {
        KeyValueStore store;
        Stream<Pair<URI, KeyToPath>> keyToPathStream =
                config.getRemotes()
                        .stream()
                        .flatMap(uri -> Stream.of(
                                Pair.of(uri, new KeyToHashURI(uri)),
                                Pair.of(uri, new KeyTo3LevelPath(uri)),
                                Pair.of(uri, new KeyTo1LevelPath(uri)),
                                Pair.of(uri, new KeyTo1LevelSoftwareHeritagePath(uri)),
                                Pair.of(uri, new KeyTo1LevelSoftwareHeritageAutoDetectPath(uri)),
                                Pair.of(uri, new KeyTo1LevelZenodoBucket(new KeyTo1LevelZenodoPath(uri, getDerefStream(uri, config.getProgressListener())))),
                                Pair.of(uri, new KeyTo1LevelZenodoBucket(new KeyTo1LevelZenodoPath(uri, getDerefStream(uri, config.getProgressListener()), KeyTo1LevelZenodoPath.ZENODO_API_PREFIX_2023_10_13, KeyTo1LevelZenodoPath.ZENODO_API_SUFFIX_2023_10_13))),
                                Pair.of(uri, new KeyTo1LevelDataOnePath(uri, getDerefStream(uri, config.getProgressListener()))),
                                Pair.of(uri, new KeyTo1LevelOCIPath(uri)),
                                Pair.of(uri, new KeyTo1LevelWikiMediaCommonsPath(uri, getDerefStream(uri, config.getProgressListener()))),
                                Pair.of(uri, new KeyTo1LevelDataVersePath(uri, getDerefStream(uri, config.getProgressListener())))
                        ));

        List<KeyValueStoreReadOnly> keyValueStoreRemotes =
                config.isSupportTarGzDiscovery()
                        ? includeTarGzSupport(keyToPathStream, keyValueStore, config.getRemotes(), config.getHashType(), config.getProgressListener(), config.isCacheEnabled())
                        : defaultRemotePathSupport(keyToPathStream, config.getProgressListener()).collect(Collectors.toList());


        if (config.isCacheEnabled()) {
            KeyValueStoreStickyFailover source = createStickyFailoverWithValidatedCache(
                    kvStreamFactory,
                    keyValueStoreRemotes,
                    config.getTmpDir(),
                    keyValueStore
            );
            store = new KeyValueStoreCopying(
                    source,
                    keyValueStore
            );
        } else {
            KeyValueStoreStickyFailover failover = new KeyValueStoreStickyFailover(keyValueStoreRemotes);
            store = new KeyValueStoreWithFallback(
                    keyValueStore,
                    failover
            );
        }
        return store;
    }

    private static Stream<KeyValueStoreReadOnly> defaultRemotePathSupport(Stream<Pair<URI, KeyToPath>> keyToPathStream, DerefProgressListener progressListener) {
        return keyToPathStream.map(x -> withStoreAt(x.getKey(), x.getValue(), progressListener));
    }

    private static List<KeyValueStoreReadOnly> includeTarGzSupport(
            Stream<Pair<URI, KeyToPath>> keyToPathStream,
            KeyValueStore keyValueStore,
            List<URI> remotes,
            HashType hashType,
            DerefProgressListener progressListener, boolean cacheEnabled) {
        return Stream.concat(
                defaultRemotePathSupport(keyToPathStream, progressListener),
                tarGzRemotePathSupport(
                        hashType,
                        remotes,
                        keyValueStore,
                        progressListener,
                        cacheEnabled)
        ).collect(Collectors.toList());
    }

    private static Stream<KeyValueStoreReadOnly> tarGzRemotePathSupport(
            HashType hashType,
            List<URI> remotes,
            KeyValueStore keyValueStore,
            DerefProgressListener progressListener,
            boolean cacheEnabled) {
        return remotes.stream().flatMap(uri -> Stream.of(
                getKeyValueStoreReadOnly(uri, new KeyTo3LevelZipPath(uri, hashType), keyValueStore, cacheEnabled, progressListener, hashType),
                getKeyValueStoreReadOnly(uri, new KeyTo3LevelImplicitZipPath(uri, hashType), keyValueStore, cacheEnabled, progressListener, hashType),
                getKeyValueStoreReadOnly(uri, new KeyTo3LevelTarGzPathShorter(uri, hashType), keyValueStore, cacheEnabled, progressListener, hashType),
                getKeyValueStoreReadOnly(uri, new KeyTo3LevelTarGzPathShort(uri, hashType), keyValueStore, cacheEnabled, progressListener, hashType),
                getKeyValueStoreReadOnly(uri, new KeyTo3LevelTarGzPath(uri, hashType), keyValueStore, cacheEnabled, progressListener, hashType)
        ));
    }

    private static KeyValueStoreReadOnly getKeyValueStoreReadOnly(URI uri, KeyToPath keyToPath, KeyValueStore keyValueStore, boolean cacheEnabled, DerefProgressListener progressListener, HashType hashType) {
        if (cacheEnabled) {
            return remoteWithTarGzCacheAll(uri, keyValueStore, keyToPath, progressListener, hashType);
        } else {
            return remoteWithTarGz(uri, keyToPath, progressListener);
        }
    }

    private static KeyValueStoreReadOnly withStoreAt(URI baseURI, KeyToPath keyToPath, DerefProgressListener progressListener) {
        return withStoreAt(keyToPath, getDerefStream(baseURI, progressListener));
    }

    private static KeyValueStoreReadOnly withStoreAt(KeyToPath keyToPath, Dereferencer<InputStream> dereferencer) {
        return new KeyValueStoreWithDereferencing(keyToPath, dereferencer);
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

    public static Dereferencer<InputStream> getInputStreamDereferencerFile(DerefProgressListener listener) {
        return uri -> {
            try {
                File file = new File(URI.create(uri.getIRIString()));
                return file.exists()
                        ? getInputStreamForFile(uri, file, listener)
                        : null;
            } catch (IllegalArgumentException ex) {
                // will not dereference malformed or non-file URI
                // see https://github.com/bio-guoda/preston/issues/291
                return null;
            }
        };
    }

    private static InputStream getInputStreamForFile(IRI uri, File file, DerefProgressListener listener) throws FileNotFoundException {
        return ContentStreamUtil.getInputStreamWithProgressLogger(uri, listener, file.length(), IOUtils.buffer(new FileInputStream(file)));
    }

    public static Dereferencer<InputStream> getDerefStreamHTTP(final DerefProgressListener listener) {
        return uri -> ResourcesHTTP.asInputStreamIgnore40x50x(uri, listener);
    }

    private static KeyValueStoreReadOnly remoteWithTarGz(
            URI baseURI,
            KeyToPath keyToPath,
            DerefProgressListener progressListener) {
        return withStoreAt(keyToPath,
                new DereferencerContentAddressedTarGZ(getDerefStream(baseURI, progressListener)));
    }

    private static KeyValueStoreReadOnly remoteWithTarGzCacheAll(
            URI baseURI,
            KeyValueStore keyValueStore,
            KeyToPath keyToPath,
            DerefProgressListener progressListener,
            HashType hashType) {
        DereferencerContentAddressedTarGZ dereferencer =
                new DereferencerContentAddressedTarGZ(
                        getDerefStream(baseURI, progressListener),
                        new BlobStoreAppendOnly(keyValueStore, false, hashType));

        return withStoreAt(keyToPath, dereferencer);
    }

}
