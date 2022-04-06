package bio.guoda.preston.store;

import org.apache.commons.rdf.api.IRI;

import java.io.IOException;

public interface ProvenanceTracker {

    void findDescendants(IRI provenanceAnchor, VersionListener listener) throws IOException;

    void findOrigins(IRI provenanceAnchor, VersionListener listener) throws IOException;


}
