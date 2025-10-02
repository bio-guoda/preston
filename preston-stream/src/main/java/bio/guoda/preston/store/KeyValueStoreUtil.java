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
                                Pair.of(uri, new KeyTo1LevelZenodoByAnchor(new KeyTo1LevelZenodoDataPaths(uri, getDerefStream(uri, config.getProgressListener())), config.getAnchor())),
                                Pair.of(uri, new KeyTo1LevelZenodoBucket(new KeyTo1LevelZenodoPath(uri, getDerefStream(uri, config.getProgressListener()), KeyTo1LevelZenodoPath.ZENODO_API_PREFIX_2023_10_13, KeyTo1LevelZenodoPath.ZENODO_API_SUFFIX_2023_10_13))),
                                Pair.of(uri, new KeyTo1LevelZenodoByAnchor(new KeyTo1LevelZenodoDataPaths(uri, getDerefStream(uri, config.getProgressListener()), KeyTo1LevelZenodoPath.ZENODO_API_PREFIX_2023_10_13, KeyTo1LevelZenodoPath.ZENODO_API_SUFFIX_2023_10_13), config.getAnchor())),
                                Pair.of(uri, new KeyTo1LevelDataOnePath(uri, getDerefStream(uri, config.getProgressListener()))),
                                Pair.of(uri, new KeyTo1LevelOCIPath(uri)),
                                Pair.of(uri, new KeyTo1LevelWikiMediaCommonsPath(uri, getDerefStream(uri, config.getProgressListener()))),
                                Pair.of(uri, new KeyTo1LevelDataVersePath(uri, getDerefStream(uri, config.getProgressListener())))
                        ));

        List<KeyValueStoreReadOnly> keyValueStoreRemotes =
                config.isSupportTarGzDiscovery()
                        ? includeTarGzSupport(keyToPathStream, keyValueStore, config)
                        : defaultRemotePathSupport(keyToPathStream, config).collect(Collectors.toList());


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

    private static Stream<KeyValueStoreReadOnly> defaultRemotePathSupport(
            Stream<Pair<URI, KeyToPath>> keyToPathStream,
            KeyValueStoreConfig config) {
        return keyToPathStream.map(
                x ->
                        withStoreAt(
                                x.getKey(),
                                x.getValue(),
                                config.getProgressListener()
                        )
        );
    }

    private static List<KeyValueStoreReadOnly> includeTarGzSupport(
            Stream<Pair<URI, KeyToPath>> keyToPathStream,
            KeyValueStore keyValueStore,
            KeyValueStoreConfig config
    ) {
        return Stream.concat(
                defaultRemotePathSupport(
                        keyToPathStream,
                        config
                ),
                tarGzRemotePathSupport(
                        keyValueStore,
                        config
                )
        ).collect(Collectors.toList());
    }

    private static Stream<KeyValueStoreReadOnly> tarGzRemotePathSupport(
            KeyValueStore keyValueStore,
            KeyValueStoreConfig config) {
        return config.getRemotes().stream().flatMap(remote -> Stream.of(
                getKeyValueStoreReadOnly(remote, new KeyTo3LevelZipPath(remote, config.getHashType()), keyValueStore, config.isCacheEnabled(), config.getProgressListener(), config.getHashType()),
                getKeyValueStoreReadOnly(remote, new KeyTo3LevelZipPathImplicit(remote, config.getHashType()), keyValueStore, config.isCacheEnabled(), config.getProgressListener(), config.getHashType()),
                getKeyValueStoreReadOnly(remote, new KeyTo3LevelZipPathExplicit(remote, config.getHashType()), keyValueStore, config.isCacheEnabled(), config.getProgressListener(), config.getHashType()),
                getKeyValueStoreReadOnly(remote, new KeyTo3LevelTarGzPathShorter(remote, config.getHashType()), keyValueStore, config.isCacheEnabled(), config.getProgressListener(), config.getHashType()),
                getKeyValueStoreReadOnly(remote, new KeyTo3LevelTarGzPathShort(remote, config.getHashType()), keyValueStore, config.isCacheEnabled(), config.getProgressListener(), config.getHashType()),
                getKeyValueStoreReadOnly(remote, new KeyTo3LevelTarGzPath(remote, config.getHashType()), keyValueStore, config.isCacheEnabled(), config.getProgressListener(), config.getHashType())
        ));
    }

    private static KeyValueStoreReadOnly getKeyValueStoreReadOnly(URI remote, KeyToPath keyToPath, KeyValueStore keyValueStore, boolean cacheEnabled, DerefProgressListener progressListener, HashType hashType) {
        if (cacheEnabled) {
            return remoteWithTarGzCacheAll(remote, keyValueStore, keyToPath, progressListener, hashType);
        } else {
            return remoteWithTarGz(remote, keyToPath, progressListener);
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
