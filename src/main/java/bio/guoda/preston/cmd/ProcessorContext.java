package bio.guoda.preston.cmd;

import bio.guoda.preston.process.StatementListener;

public interface ProcessorContext {
    ProcessorState getState();

    StatementListener[] getListeners();
}
