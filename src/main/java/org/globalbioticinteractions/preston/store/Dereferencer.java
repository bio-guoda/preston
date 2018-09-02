package org.globalbioticinteractions.preston.store;

import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public interface Dereferencer {
    InputStream dereference(IRI uri) throws IOException;
}
