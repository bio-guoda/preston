package bio.guoda.preston.store;

import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;

public interface KeyValueStoreReadOnly {
    InputStream get(IRI key) throws IOException;
}
