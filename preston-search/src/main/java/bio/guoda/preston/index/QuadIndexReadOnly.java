package bio.guoda.preston.index;

import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDFTerm;

import java.io.IOException;
import java.util.stream.Stream;

public interface QuadIndexReadOnly {
    Stream<Quad> findQuadsWithSubject(BlankNodeOrIRI subject, int maxHits) throws IOException;

    Stream<Quad> findQuadsWithPredicate(IRI predicate, int maxHits) throws IOException;

    Stream<Quad> findQuadsWithObject(RDFTerm object, int maxHits) throws IOException;

    Stream<Quad> findQuadsWithGraphName(BlankNodeOrIRI graphName, int maxHits) throws IOException;
}
