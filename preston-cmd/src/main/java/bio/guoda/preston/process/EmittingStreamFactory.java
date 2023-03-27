package bio.guoda.preston.process;

/**
 * Emitting Stream Factory generates objects that take an inputStream and emit *statements* from them.
 */

public interface EmittingStreamFactory {

    ParsingEmitter createEmitter(StatementEmitter emitter, ProcessorState context);

}
