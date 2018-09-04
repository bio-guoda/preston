package bio.guoda.preston.process;

import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;

public interface BlobStoreReadOnly {
    InputStream get(IRI key) throws IOException;
}
