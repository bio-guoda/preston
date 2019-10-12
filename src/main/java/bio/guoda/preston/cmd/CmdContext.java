package bio.guoda.preston.cmd;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.process.StatementListener;
import org.apache.commons.rdf.api.IRI;

public class CmdContext implements ProcessorContext {

    private final ProcessorState state;
    private IRI provRoot;
    private StatementListener[] listeners;

    CmdContext(ProcessorState state, StatementListener... listeners) {
        this(state, RefNodeConstants.ARCHIVE, listeners);
    }

    CmdContext(ProcessorState state, IRI provRoot, StatementListener... listeners) {
        this.state = state;
        this.provRoot = provRoot;
        this.listeners = listeners;
    }

    @Override
    public ProcessorState getState() {
        return state;
    }

    @Override
    public StatementListener[] getListeners() {
        return listeners;
    }

    public IRI getProvRoot() {
        return provRoot;
    }
}
