package bio.guoda.preston.cmd;

import bio.guoda.preston.process.ProcessorState;

public class ProcessorStateAlwaysContinue implements ProcessorState {
    @Override
    public boolean shouldKeepProcessing() {
        return true;
    }

    @Override
    public void stopProcessing() {

    }
}