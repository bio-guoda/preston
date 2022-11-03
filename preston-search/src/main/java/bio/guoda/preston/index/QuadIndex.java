package bio.guoda.preston.index;

import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

public interface QuadIndex extends QuadIndexReadOnly{
    void put(Quad quad, IRI origin);
}
