package bio.guoda.preston.process;

import org.apache.commons.rdf.api.Quad;

import java.util.List;

public abstract class StatementsListenerEmitterAdapter implements StatementsListener, StatementsEmitter {

    @Override
    public void on(List<Quad> statements) {
        for (Quad statement : statements) {
            on(statement);
        }
    }

    @Override
    public void emit(List<Quad> statements) {
        for (Quad statement : statements) {
            emit(statement);
        }
    }
}
