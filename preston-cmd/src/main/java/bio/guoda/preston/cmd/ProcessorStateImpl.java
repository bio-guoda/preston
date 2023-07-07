package bio.guoda.preston.cmd;

import bio.guoda.preston.process.ProcessorState;

import java.util.concurrent.atomic.AtomicBoolean;

public class ProcessorStateImpl implements ProcessorState {
    private AtomicBoolean shouldKeepProcessing = new AtomicBoolean(true);

    @Override
    public void stopProcessing() {
        shouldKeepProcessing.set(false);
    }

    @Override
    public boolean shouldKeepProcessing() {
        return shouldKeepProcessing.get();
    }
}
