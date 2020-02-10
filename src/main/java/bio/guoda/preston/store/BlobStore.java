package bio.guoda.preston.store;

import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDFTerm;
import bio.guoda.preston.process.BlobStoreReadOnly;

import java.io.IOException;
import java.io.InputStream;

public interface BlobStore extends BlobStoreReadOnly {

    IRI put(InputStream is) throws IOException;

}
