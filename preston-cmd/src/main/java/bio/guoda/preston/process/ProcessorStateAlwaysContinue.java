package bio.guoda.preston.process;

public class ProcessorStateAlwaysContinue implements ProcessorState {
    @Override
    public boolean shouldKeepProcessing() {
        return true;
    }

    @Override
    public void stopProcessing() {

    }
}
