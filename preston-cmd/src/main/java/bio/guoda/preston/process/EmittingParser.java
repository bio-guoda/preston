package bio.guoda.preston.process;

import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;

public interface EmittingParser {
    void parse(InputStream is, StatementsEmitter emitter, BlankNodeOrIRI versionSource) throws IOException;
}
