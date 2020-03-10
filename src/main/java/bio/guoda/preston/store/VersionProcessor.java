package bio.guoda.preston.store;

import bio.guoda.preston.process.StatementListener;
import bio.guoda.preston.process.StatementProcessor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.rdf.api.BlankNode;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.Triple;
import org.apache.commons.rdf.api.Quad;

import java.io.IOException;

import static bio.guoda.preston.model.RefNodeFactory.getVersion;


public abstract class VersionProcessor extends StatementProcessor {
    private static Log LOG = LogFactory.getLog(VersionProcessor.class);

    public VersionProcessor(StatementListener... listener) {
        super(listener);
    }

    @Override
    public void on(Quad statement) {
        try {
            BlankNodeOrIRI version = getVersion(statement);
            if (version instanceof BlankNode) {
                handleBlankVersion(statement, (BlankNode) version);
            }
        } catch (Throwable e) {
            LOG.warn("failed to handle [" + statement.toString() + "]", e);
        }

    }

    abstract void handleBlankVersion(Quad statement, BlankNode blankVersion) throws IOException;

}
