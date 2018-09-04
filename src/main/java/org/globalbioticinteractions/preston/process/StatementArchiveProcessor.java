package org.globalbioticinteractions.preston.process;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.api.Triple;
import org.apache.commons.rdf.jena.JenaRDF;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.core.Quad;
import org.globalbioticinteractions.preston.RefNodeConstants;
import org.globalbioticinteractions.preston.model.RefNodeFactory;

import java.io.IOException;
import java.io.InputStream;

public class StatementArchiveProcessor extends ProcessorReadOnly {

    private static final Log LOG = LogFactory.getLog(StatementArchiveProcessor.class);

    public StatementArchiveProcessor(BlobStoreReadOnly blobStoreReadOnly, StatementListener... listeners) {
        super(blobStoreReadOnly, listeners);
    }

    @Override
    public void on(Triple statement) {
        if (statement.getSubject().equals(RefNodeConstants.ARCHIVE_COLLECTION_IRI)
                && RefNodeFactory.hasVersionAvailable(statement)) {

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

        @Override
        public void start() {

        }

        @Override
        public void triple(org.apache.jena.graph.Triple triple) {
            Triple triple1 = JenaRDF.asTriple(rdf, triple);
            emit(triple1);
        }

        @Override
        public void quad(Quad quad) {
            org.apache.commons.rdf.api.Quad quad1 = JenaRDF.asQuad(rdf, quad);
            emit(quad1.asTriple());
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
