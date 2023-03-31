package bio.guoda.preston.store;

import bio.guoda.preston.process.ProcessorState;
import bio.guoda.preston.process.StatementListener;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;

public interface ProvenanceTracer extends ProcessorState {

    void trace(IRI provenanceAnchor, StatementListener listener) throws IOException;

}
