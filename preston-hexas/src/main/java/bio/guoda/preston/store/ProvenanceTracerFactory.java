package bio.guoda.preston.store;

import org.apache.commons.rdf.api.IRI;

import java.util.List;

public interface ProvenanceTracerFactory {

    ProvenanceTracer create(List<IRI> provenanceAnchors);


}
