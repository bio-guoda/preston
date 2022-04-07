package bio.guoda.preston.store;

import bio.guoda.preston.process.StatementListener;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;

public interface ProvenanceTracker {

    void findDescendants(IRI provenanceAnchor, StatementListener listener) throws IOException;

    void findOrigins(IRI provenanceAnchor, StatementListener listener) throws IOException;


}
