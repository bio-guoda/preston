package bio.guoda.preston.process;

import bio.guoda.preston.RDFUtil;
import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.VersionUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.simple.SimpleRDF;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import static bio.guoda.preston.RefNodeFactory.toIRI;

public class EmittingStreamOfVersions {
    private final RDF rdfSimple = new SimpleRDF();
    private final StatementsEmitter emitter;
    private final ProcessorState context;

    public EmittingStreamOfVersions(StatementsEmitter emitter) {
        this(emitter, new ProcessorStateAlwaysContinue());
    }

    public EmittingStreamOfVersions(StatementsEmitter emitter,
                                    ProcessorState processorState) {
        this.emitter = emitter;
        this.context = processorState;
    }

    public void parseAndEmit(InputStream inputStream) {
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        try {
            String line;
            while (context.shouldKeepProcessing() && (line = reader.readLine()) != null) {
                IRI iri = VersionUtil.mostRecentVersion(line);
                if (iri == null && VersionUtil.maybeNotQuad(line)) {
                    iri = toIRI(StringUtils.trim(line));
                }
                if (iri != null) {
                    Quad quad = RefNodeFactory.toStatement(RefNodeFactory.toBlank(), RefNodeConstants.HAS_VERSION, iri);
                    copyOnEmit(quad);
                }
            }
        } catch (IOException ex) {
            // ignore
        }
    }


    private void copyOnEmit(org.apache.commons.rdf.api.Quad quad) {
        org.apache.commons.rdf.api.Quad copyOfTriple = rdfSimple.createQuad(
                quad.getGraphName().orElse(null),
                quad.getSubject(),
                quad.getPredicate(),
                quad.getObject());
        emitter.emit(copyOfTriple);
    }

}
