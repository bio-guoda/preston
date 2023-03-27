package bio.guoda.preston.process;

import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.simple.SimpleRDF;

public abstract class EmittingStreamAbstract implements ParsingEmitter {

    private final RDF rdfSimple = new SimpleRDF();
    private final StatementEmitter emitter;
    private final ProcessorState context;

    public EmittingStreamAbstract(StatementEmitter emitter) {
        this(emitter, new ProcessorStateAlwaysContinue());
    }

    public EmittingStreamAbstract(StatementEmitter emitter,
                                  ProcessorState processorState) {
        this.emitter = emitter;
        this.context = processorState;
    }

    protected void copyOnEmit(Quad quad) {
        Quad copyOfTriple = rdfSimple.createQuad(
                quad.getGraphName().orElse(null),
                quad.getSubject(),
                quad.getPredicate(),
                quad.getObject());
        emitter.emit(copyOfTriple);
    }


    public ProcessorState getContext() {
        return context;
    }
}
