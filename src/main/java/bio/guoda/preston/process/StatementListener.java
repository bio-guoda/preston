package bio.guoda.preston.process;

import org.apache.commons.rdf.api.TripleLike;

public interface StatementListener {
    void on(TripleLike statement);
}
