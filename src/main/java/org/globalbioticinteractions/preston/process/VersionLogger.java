package org.globalbioticinteractions.preston.process;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.api.Triple;
import org.apache.commons.rdf.jena.JenaRDF;
import org.apache.commons.rdf.simple.SimpleRDF;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.core.Quad;
import org.globalbioticinteractions.preston.model.RefNodeFactory;

import java.io.IOException;
import java.io.InputStream;

public class VersionLogger extends ProcessorReadOnly {

    private static final Log LOG = LogFactory.getLog(VersionLogger.class);

    public VersionLogger(BlobStoreReadOnly blobStoreReadOnly, StatementListener... listeners) {
        super(blobStoreReadOnly, listeners);
    }

    @Override
    public void on(Triple statement) {
        if (RefNodeFactory.hasVersionAvailable(statement)) {

            IRI version = (IRI) RefNodeFactory.getVersion(statement);
            try {
                InputStream inputStream = get(version);
                RDFDataMgr.parse(new EmittingStreamRDF(), inputStream, Lang.NQUADS);
            } catch (IOException e) {
                LOG.warn("failed to read archive [" + RefNodeFactory.getVersion(statement) + "]", e);
            }

        }
    }

    private class EmittingStreamRDF implements StreamRDF {
        RDF rdf = new JenaRDF();
        RDF rdfSimmple = new SimpleRDF();

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
            Triple copyOfTriple = rdfSimmple.createTriple(triple.getSubject(), triple.getPredicate(), triple.getObject());
            emit(copyOfTriple);
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
}
