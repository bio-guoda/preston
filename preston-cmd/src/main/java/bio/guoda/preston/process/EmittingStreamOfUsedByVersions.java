package bio.guoda.preston.process;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.VersionUtil;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import static bio.guoda.preston.RefNodeFactory.toIRI;

public class EmittingStreamOfUsedByVersions extends EmittingStreamAbstract {

    public EmittingStreamOfUsedByVersions(StatementsEmitter emitter, ProcessorState processorState) {
        super(emitter, processorState);
    }

    public void parseAndEmit(InputStream inputStream) {
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        try {
            String line;
            while (getContext().shouldKeepProcessing() && (line = reader.readLine()) != null) {
                IRI iri = VersionUtil.mostRecentVersionUsedBy(line);
                if (iri != null) {
                    Quad quad = RefNodeFactory.toStatement(iri, RefNodeConstants.USED_BY, RefNodeFactory.toBlank());
                    copyOnEmit(quad);
                }
            }
        } catch (IOException ex) {
            // ignore
        }
    }


}
