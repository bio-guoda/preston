package bio.guoda.preston.process;

import org.apache.commons.rdf.api.Quad;

public interface StatementListener {
    void on(Quad statement);
}
