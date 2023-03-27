package bio.guoda.preston.process;

public interface EmittingStreamFactory {

    ParsingEmitter createEmitter(StatementEmitter emitter, ProcessorState context);

}
