package org.globalbioticinteractions.preston.process;

import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public interface BlobStoreReadOnly {
    InputStream get(IRI key) throws IOException;
}
