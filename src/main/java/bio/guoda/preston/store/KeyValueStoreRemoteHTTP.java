package bio.guoda.preston.store;

import bio.guoda.preston.model.RefNodeFactory;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;


public class KeyValueStoreRemoteHTTP implements KeyValueStoreReadOnly {

    private final KeyToPath keyToPath;
    private final Dereferencer3<InputStream> dereferencer;

    public KeyValueStoreRemoteHTTP(KeyToPath keyToPath, Dereferencer3<InputStream> dereferencer) {
        this.keyToPath = keyToPath;
        this.dereferencer = dereferencer;
    }

    @Override
    public InputStream get(String key) throws IOException {
        IRI uri = RefNodeFactory.toIRI(keyToPath.toPath(key));
        return dereferencer.dereference(uri);
    }


}
