package bio.guoda.preston.store;

import bio.guoda.preston.RefNodeFactory;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;


public class KeyValueStoreWithDereferencing implements KeyValueStoreReadOnly {

    private final KeyToPath keyToPath;
    private final Dereferencer<InputStream> dereferencer;

    public KeyValueStoreWithDereferencing(KeyToPath keyToPath, Dereferencer<InputStream> dereferencer) {
        this.keyToPath = keyToPath;
        this.dereferencer = dereferencer;
    }

    @Override
    public InputStream get(IRI key) throws IOException {
        IRI uri = RefNodeFactory.toIRI(keyToPath.toPath(key));
        return dereferencer.get(uri);
    }


}
