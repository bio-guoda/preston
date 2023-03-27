package bio.guoda.preston.process;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.VersionUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import static bio.guoda.preston.RefNodeFactory.toIRI;

public class EmittingStreamOfAnyVersions extends EmittingStreamAbstract {

    public EmittingStreamOfAnyVersions(StatementsEmitter emitter) {
        super(emitter);
    }

    public EmittingStreamOfAnyVersions(StatementEmitter emitter, ProcessorState processorState) {
        super(emitter, processorState);
    }

    @Override
    public void parseAndEmit(InputStream inputStream) {
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        try {
            String line;
            while (getContext().shouldKeepProcessing() && (line = reader.readLine()) != null) {
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


}
