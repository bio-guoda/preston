package bio.guoda.preston.process;

import bio.guoda.preston.RefNodeConstants;
import org.apache.commons.rdf.api.IRI;

public class CmdContext implements ProcessorContext {

    private final ProcessorState state;
    private IRI provRoot;
    private StatementsListener[] listeners;

    CmdContext(ProcessorState state, StatementsListener... listeners) {
        this(state, RefNodeConstants.BIODIVERSITY_DATASET_GRAPH, listeners);
    }

    CmdContext(ProcessorState state, IRI provRoot, StatementsListener... listeners) {
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
