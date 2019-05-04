package bio.guoda.preston.store;

import bio.guoda.preston.Resources;
import bio.guoda.preston.model.RefNodeFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;


public class KeyValueStoreRemoteHTTP implements KeyValueStoreReadOnly {

    private final URL baseUrl;
    private final KeyToPath keyToPath;
    private final Dereferencer dereferencer;

    public KeyValueStoreRemoteHTTP(URL baseUrl) {
        this(baseUrl, new KeyTo1LevelPath(), Resources::asInputStreamIgnore404);
    }

    public KeyValueStoreRemoteHTTP(URL baseUrl, KeyToPath keyToPath, Dereferencer deferencer) {
        this.baseUrl = baseUrl;
        this.keyToPath = keyToPath;
        this.dereferencer = deferencer;
    }

    @Override
    public InputStream get(String key) throws IOException {
        return dereferencer.dereference(RefNodeFactory.toIRI(baseUrl.toExternalForm() + keyToPath.toPath(key)));
    }


}
