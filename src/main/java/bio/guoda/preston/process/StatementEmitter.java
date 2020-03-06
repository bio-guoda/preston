package bio.guoda.preston.process;


import org.apache.commons.rdf.api.TripleLike;

public interface StatementEmitter {
    void emit(TripleLike statement);
}
