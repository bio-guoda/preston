package bio.guoda.preston.process;


import org.apache.commons.rdf.api.Quad;

import java.util.List;

public abstract class StatementsEmitterAdapter implements StatementsEmitter {

    @Override
    public void emit(List<Quad> statements) {
        for (Quad statement : statements) {
            emit(statement);
        }
    }
}
