package bio.guoda.preston.process;


import org.apache.commons.rdf.api.Quad;

public interface StatementEmitter {
    void emit(Quad statement);
}
