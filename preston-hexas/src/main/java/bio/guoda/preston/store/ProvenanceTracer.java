package bio.guoda.preston.store;

import bio.guoda.preston.process.StatementListener;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;

public interface ProvenanceTracer {

    void trace(IRI provenanceAnchor, StatementListener listener) throws IOException;

}
