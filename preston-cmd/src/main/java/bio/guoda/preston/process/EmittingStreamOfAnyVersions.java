package bio.guoda.preston.process;

import bio.guoda.preston.store.VersionUtil;
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
                Quad aQuad = VersionUtil.parseAsVersionStatementOrNull(line);
                if (aQuad != null) {
                    copyOnEmit(aQuad);
                }
            }
        } catch (IOException ex) {
            // ignore
        }
    }


}
