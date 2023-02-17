package bio.guoda.preston.process;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.VersionUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.simple.SimpleRDF;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import static bio.guoda.preston.RefNodeFactory.toIRI;

public class EmittingStreamOfAnyVersions {
    private final RDF rdfSimple = new SimpleRDF();
    private final StatementsEmitter emitter;
    private final ProcessorState context;

    public EmittingStreamOfAnyVersions(StatementsEmitter emitter) {
        this(emitter, new ProcessorStateAlwaysContinue());
    }

    public EmittingStreamOfAnyVersions(StatementsEmitter emitter,
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
                    try {
                        iri = toIRI(StringUtils.trim(line));
                    } catch (IllegalArgumentException ex) {
                        // simply ignore invalid IRIs
                    }
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
