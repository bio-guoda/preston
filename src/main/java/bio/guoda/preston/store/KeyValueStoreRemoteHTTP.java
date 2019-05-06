package bio.guoda.preston.store;

import bio.guoda.preston.Resources;
import bio.guoda.preston.model.RefNodeFactory;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;


public class KeyValueStoreRemoteHTTP implements KeyValueStoreReadOnly {

    private final KeyToPath keyToPath;
    private final Dereferencer dereferencer;

    public KeyValueStoreRemoteHTTP(URI baseUrl) {
        this(new KeyTo1LevelPath(baseUrl), Resources::asInputStreamIgnore404);
    }

    public KeyValueStoreRemoteHTTP(KeyToPath keyToPath, Dereferencer deferencer) {
        this.keyToPath = keyToPath;
        this.dereferencer = deferencer;
    }

    @Override
    public InputStream get(String key) throws IOException {
        IRI uri = RefNodeFactory.toIRI(keyToPath.toPath(key));
        return dereferencer.dereference(uri);
    }


}
