package bio.guoda.preston.process;


import bio.guoda.preston.cmd.ProcessorState;
import org.apache.commons.rdf.api.Quad;

public abstract class StatementProcessor extends StatementsListenerEmitterAdapter implements StatementsListener, StatementsEmitter {

    private final StatementsListener[] listeners;
    private final ProcessorState state;

    public StatementProcessor(StatementsListener... listeners) {
        this(() -> true, listeners);
    }

    public StatementProcessor(ProcessorState state, StatementsListener... listeners) {
        this.listeners = listeners;
        this.state = state;
    }

    @Override
    public void emit(Quad statement) {
        for (StatementsListener listener : listeners) {
            listener.on(statement);
        }
    }

    public ProcessorState getState() {
        return state;
    }

}
