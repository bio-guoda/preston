package bio.guoda.preston.store;

import org.apache.commons.rdf.api.IRI;

import java.io.IOException;

public interface Dereferencer3<T> {

    T dereference(IRI uri) throws IOException;

}
