package bio.guoda.preston.store;

import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;

public interface KeyValueStore extends KeyValueStoreReadOnly {

    IRI put(KeyGeneratingStream keyGeneratingStream, InputStream is) throws IOException;

    void put(IRI key, InputStream is) throws IOException;

}
