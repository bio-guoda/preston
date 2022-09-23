package bio.guoda.preston.store;

import org.apache.commons.rdf.api.IRI;

import java.io.IOException;

public interface KeyValueStoreWithRemove extends KeyValueStore {

    void remove(IRI key) throws IOException;

}
