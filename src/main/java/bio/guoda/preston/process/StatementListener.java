package bio.guoda.preston.process;

import bio.guoda.preston.util.ValueListener;
import org.apache.commons.rdf.api.Quad;

public interface StatementListener extends ValueListener<Quad> {
    void on(Quad statement);
}
