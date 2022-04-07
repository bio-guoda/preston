package bio.guoda.preston.process;

import org.apache.commons.rdf.api.Quad;

public interface StatementListener extends ValueListener<Quad> {
    void on(Quad statement);
}
