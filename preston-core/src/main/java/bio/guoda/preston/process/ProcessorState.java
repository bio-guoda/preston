package bio.guoda.preston.process;

public interface ProcessorState extends ProcessorStateReadOnly {
    void stopProcessing();
}
