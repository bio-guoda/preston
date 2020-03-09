package bio.guoda.preston.process;


import bio.guoda.preston.cmd.ProcessorState;
import org.apache.commons.rdf.api.Triple;
import org.apache.commons.rdf.api.Quad;

public abstract class StatementProcessor implements StatementListener, StatementEmitter {

    private final StatementListener[] listeners;
    private final ProcessorState state;

    public StatementProcessor(StatementListener... listeners) {
        this(() -> true, listeners);
    }

    public StatementProcessor(ProcessorState state, StatementListener... listeners) {
        this.listeners = listeners;
        this.state = state;
    }

    @Override
    public void emit(Quad statement) {
        for (StatementListener listener : listeners) {
            listener.on(statement);
        }
    }

    public ProcessorState getState() {
        return state;
    }
}
