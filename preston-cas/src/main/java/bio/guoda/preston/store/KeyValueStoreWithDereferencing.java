package bio.guoda.preston.store;

import bio.guoda.preston.RefNodeFactory;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;


public class KeyValueStoreWithDereferencing implements KeyValueStoreReadOnly {

    private final KeyToPath keyToPath;
    private final Dereferencer<InputStream> dereferencer;

    public KeyValueStoreWithDereferencing(KeyToPath keyToPath, Dereferencer<InputStream> dereferencer) {
        this.keyToPath = keyToPath;
        this.dereferencer = dereferencer;
    }

    @Override
    public InputStream get(IRI key) throws IOException {
        InputStream is = null;

        if (keyToPath.supports(key)) {
            URI uri = keyToPath.toPath(key);
            is = uri == null ? null : dereferencer.get(RefNodeFactory.toIRI(uri));
        }
        return is;
    }


}
