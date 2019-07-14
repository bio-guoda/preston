package bio.guoda.preston.process;

import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.api.Triple;
import org.apache.commons.rdf.jena.JenaRDF;
import org.apache.commons.rdf.simple.SimpleRDF;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.core.Quad;

import java.io.InputStream;

public class EmittingStreamRDF implements StreamRDF {
    private final RDF rdf = new JenaRDF();
    private final RDF rdfSimple = new SimpleRDF();
    private final StatementEmitter emitter;

    public EmittingStreamRDF(StatementEmitter emitter) {
        this.emitter = emitter;
    }


    public void parseAndEmit(InputStream inputStream) {
        RDFDataMgr.parse(this, inputStream, Lang.NQUADS);
    }


    @Override
    public void start() {

    }

    @Override
    public void triple(org.apache.jena.graph.Triple triple) {
        copyOnEmit(JenaRDF.asTriple(rdf, triple));
    }

    @Override
    public void quad(Quad quad) {
        copyOnEmit(JenaRDF.asQuad(rdf, quad).asTriple());
    }

    public void copyOnEmit(Triple triple) {
        Triple copyOfTriple = rdfSimple.createTriple(triple.getSubject(), triple.getPredicate(), triple.getObject());
        emitter.emit(copyOfTriple);
    }

    @Override
    public void base(String s) {

    }

    @Override
    public void prefix(String s, String s1) {

    }

    @Override
    public void finish() {

    }
}
