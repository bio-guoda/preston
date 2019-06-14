package bio.guoda.preston.cmd;

import bio.guoda.preston.Resources;
import bio.guoda.preston.store.KeyTo1LevelPath;
import bio.guoda.preston.store.KeyTo3LevelPath;
import bio.guoda.preston.store.KeyToPath;
import bio.guoda.preston.store.KeyValueStore;
import bio.guoda.preston.store.KeyValueStoreCopying;
import bio.guoda.preston.store.KeyValueStoreReadOnly;
import bio.guoda.preston.store.KeyValueStoreRemoteHTTP;
import bio.guoda.preston.store.KeyValueStoreStickyFailover;
import bio.guoda.preston.store.KeyValueStoreWithFallback;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.URIConverter;
import org.apache.commons.rdf.api.IRI;

import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeConstants.ARCHIVE;

public class Persisting extends PersistingLocal {

    @Parameter(names = {"--remote", "--remotes"}, description = "remote graph url(s) (e.g., https://deeplinker.bio/,https://example.org)", converter = URIConverter.class, validateWith = URIValidator.class)
    private List<URI> remoteURIs;

    @Parameter(names = {"--no-cache"}, description = "disable local content cache")
    private Boolean noLocalCache = false;

    private final IRI provenanceRoot = ARCHIVE;

    public IRI getProvenanceRoot() {
        return this.provenanceRoot;
    }

    protected List<URI> getRemoteURIs() {
        return remoteURIs;
    }

    protected boolean hasRemote() {
        return remoteURIs != null && !remoteURIs.isEmpty();
    }

    @Override
    protected KeyValueStore getKeyValueStore() {
        KeyValueStore store;
        if (hasRemote()) {
            Stream<KeyToPath> keyToPathStream =
                    getRemoteURIs().stream().flatMap(uri -> Stream.of(
                            new KeyTo1LevelPath(uri),
                            new KeyTo3LevelPath(uri)));

            List<KeyValueStoreReadOnly> keyValueStoreRemotes = keyToPathStream.map(this::remoteWith).collect(Collectors.toList());
            KeyValueStoreStickyFailover failover = new KeyValueStoreStickyFailover(keyValueStoreRemotes);

            if (noLocalCache) {
                store = new KeyValueStoreWithFallback(
                        super.getKeyValueStore(),
                        failover);
            } else {
                store = new KeyValueStoreCopying(
                        failover,
                        super.getKeyValueStore());
            }
        } else {
            store = super.getKeyValueStore();
        }
        return store;
    }

    private KeyValueStoreRemoteHTTP remoteWith(KeyToPath keyToPath) {
        return new KeyValueStoreRemoteHTTP(keyToPath, Resources::asInputStreamIgnore404);
    }


}
