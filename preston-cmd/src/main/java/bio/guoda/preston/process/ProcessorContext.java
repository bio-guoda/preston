package bio.guoda.preston.process;

import bio.guoda.preston.process.ProcessorState;
import bio.guoda.preston.process.StatementsListener;

public interface ProcessorContext {
    ProcessorState getState();

    StatementsListener[] getListeners();
}
