package bio.guoda.preston.process;

import bio.guoda.preston.cmd.ProcessorState;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.api.Triple;
import org.apache.commons.rdf.jena.JenaRDF;
import org.apache.commons.rdf.simple.SimpleRDF;
import org.apache.jena.atlas.iterator.IteratorResourceClosing;
import org.apache.jena.riot.lang.LabelToNode;
import org.apache.jena.riot.lang.RiotParsers;
import org.apache.jena.riot.system.ErrorHandlerFactory;
import org.apache.jena.riot.system.FactoryRDF;
import org.apache.jena.riot.system.IRIResolver;
import org.apache.jena.riot.system.ParserProfile;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.core.Quad;

import java.io.InputStream;
import java.util.Iterator;

import static org.apache.jena.riot.system.RiotLib.createParserProfile;
import static org.apache.jena.riot.system.RiotLib.factoryRDF;

public class EmittingStreamRDF {
    private final RDF rdf = new JenaRDF();
    private final RDF rdfSimple = new SimpleRDF();
    private final StatementEmitter emitter;
    private final ProcessorState context;

    public EmittingStreamRDF(StatementEmitter emitter) {
        this(emitter, new ProcessorState() {

            @Override
            public boolean shouldKeepProcessing() {
                return true;
            }
        });
    }

    public EmittingStreamRDF(StatementEmitter emitter, ProcessorState processorState) {
        this.emitter = emitter;
        this.context = processorState;
    }

    public void parseAndEmit(InputStream inputStream) {
        FactoryRDF factory = factoryRDF(LabelToNode.createScopeByGraph());
        ParserProfile profile = createParserProfile(factory, ErrorHandlerFactory.errorHandlerStd, false);
        profile.setIRIResolver(IRIResolver.createNoResolve());
        Iterator<Quad> iteratorNQuads = RiotParsers.createIteratorNQuads(inputStream, (StreamRDF) null, profile);
        Iterator<Quad> iteratorQuads = new IteratorResourceClosing<>(iteratorNQuads, inputStream);
        while (context.shouldKeepProcessing() && iteratorQuads.hasNext()) {
            Quad nextQuad = iteratorQuads.next();
            copyOnEmit(JenaRDF.asQuad(rdf, nextQuad).asTriple());
        }
    }


    private void copyOnEmit(Triple triple) {
        Triple copyOfTriple = rdfSimple.createTriple(triple.getSubject(), triple.getPredicate(), triple.getObject());
        emitter.emit(copyOfTriple);
    }

}
