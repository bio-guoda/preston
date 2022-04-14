package bio.guoda.preston.store;


import bio.guoda.preston.process.ProcessorState;
import bio.guoda.preston.process.StatementsEmitter;
import bio.guoda.preston.process.StatementsListener;
import org.apache.commons.rdf.api.Quad;

import java.util.Collections;
import java.util.List;

public abstract class StatementProcessor extends StatementsListenerEmitterAdapter implements StatementsListener, StatementsEmitter {

    private final StatementsListener[] listeners;
    private final ProcessorState state;

    public StatementProcessor(StatementsListener... listeners) {
        this(new ProcessorStateAlwaysContinue(), listeners);
    }

    public StatementProcessor(ProcessorState state, StatementsListener... listeners) {
        this.listeners = listeners;
        this.state = state;
    }

    @Override
    public void emit(Quad statement) {
        emit(Collections.singletonList(statement));
    }

    @Override
    public void emit(List<Quad> statements) {
        for (StatementsListener listener : listeners) {
            listener.on(statements);
        }
    }

    public ProcessorState getState() {
        return state;
    }

}
