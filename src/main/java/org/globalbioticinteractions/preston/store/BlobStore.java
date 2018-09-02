package org.globalbioticinteractions.preston.store;

import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDFTerm;
import org.globalbioticinteractions.preston.process.BlobStoreReadOnly;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public interface BlobStore extends BlobStoreReadOnly {

    IRI putBlob(InputStream is) throws IOException;

    IRI putBlob(RDFTerm entity) throws IOException;

}
