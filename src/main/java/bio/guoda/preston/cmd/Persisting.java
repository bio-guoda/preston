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

    @Parameter(names = {"--remote"}, description = "remote url", converter = URIConverter.class, validateWith = URIValidator.class)
    private URI remoteURI = null;

    @Parameter(names = {"--no-cache"}, description = "cache remote content locally")
    private Boolean noLocalCache = false;

    @Parameter(names = "--prov", description = "provenance iri",
            converter = IRIConverter.class, validateWith = IRIValidator.class)
    private IRI provenanceRoot = ARCHIVE;

    public IRI getProvenanceRoot() {
        return this.provenanceRoot;
    }


    protected URI getRemoteURI() {
        return remoteURI;
    }

    protected boolean hasRemote() {
        return remoteURI != null;
    }


    @Override
    protected KeyValueStore getKeyValueStore() {
        KeyValueStore store;
        if (hasRemote()) {
            Stream<KeyToPath> keyToPathStream = getKeyToPathRemotes();
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

    protected Stream<KeyToPath> getKeyToPathRemotes() {
        return Stream.of(
                new KeyTo1LevelPath(getRemoteURI()),
                new KeyTo3LevelPath(getRemoteURI())
        );
    }

    private KeyValueStoreRemoteHTTP remoteWith(KeyToPath keyToPath) {
        return new KeyValueStoreRemoteHTTP(keyToPath, Resources::asInputStreamIgnore404);
    }




}
