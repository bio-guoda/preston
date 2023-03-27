package bio.guoda.preston.cmd;

import bio.guoda.preston.process.EmittingStreamFactory;
import bio.guoda.preston.process.EmittingStreamOfAnyQuad;
import bio.guoda.preston.process.ParsingEmitter;
import bio.guoda.preston.process.ProcessorState;
import bio.guoda.preston.process.StatementEmitter;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.ProvenanceTracer;
import org.apache.commons.rdf.api.Quad;

import java.util.function.Predicate;

public final class AliasUtil {

    public static void findSelectedAlias(StatementsListener listener, Predicate<Quad> selector, Persisting persisting) {
        ReplayUtil.replay(
                new SelectiveListener(selector, listener),
                persisting,
                getEmitterFactory()
        );
    }

    public static void findSelectedAlias(StatementsListener listener, Predicate<Quad> selector, Persisting persisting, ProvenanceTracer tracer) {
        ReplayUtil.replay(
                new SelectiveListener(selector, listener),
                persisting,
                tracer,
                getEmitterFactory()
        );
    }

    private static EmittingStreamFactory getEmitterFactory() {
        return new EmittingStreamFactory() {
            @Override
            public ParsingEmitter createEmitter(StatementEmitter emitter, ProcessorState context) {
                return new EmittingStreamOfAnyQuad(emitter, context);
            }
        };
    }

}
