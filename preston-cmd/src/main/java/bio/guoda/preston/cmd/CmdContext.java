package bio.guoda.preston.cmd;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.process.ProcessorContext;
import bio.guoda.preston.process.ProcessorState;
import bio.guoda.preston.process.StatementsListener;
import org.apache.commons.rdf.api.IRI;

public class CmdContext implements ProcessorContext {

    private final ProcessorState state;
    private IRI provRoot;
    private StatementsListener[] listeners;

    public CmdContext(ProcessorState state, IRI provRoot, StatementsListener... listeners) {
        this.state = state;
        this.provRoot = provRoot;
        this.listeners = listeners;
    }

    @Override
    public ProcessorState getState() {
        return state;
    }

    @Override
    public StatementsListener[] getListeners() {
        return listeners;
    }

    public IRI getProvRoot() {
        return provRoot;
    }
}
