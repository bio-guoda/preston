package bio.guoda.preston.store;

import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;

public interface Dereferencer {
    InputStream dereference(IRI uri) throws IOException;
}
