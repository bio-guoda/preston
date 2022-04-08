package bio.guoda.preston.store;

import org.apache.commons.rdf.api.IRI;

import java.io.IOException;

public class DereferenceException extends IOException {

    public DereferenceException(String message) {
        super(message);
    }

    public DereferenceException(String message, Throwable e) {
        super(message, e);
    }

    public DereferenceException(IRI iri) {
        this("failed to dereference [" + iri.getIRIString() + "]");
    }

    public DereferenceException(IRI iri, Throwable e) {
        this("failed to dereference [" + iri.getIRIString() + "]", e);
    }

}
