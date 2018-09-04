package bio.guoda.preston.process;


import org.apache.commons.rdf.api.Triple;

public interface StatementEmitter {
    void emit(Triple statement);
}
