package bio.guoda.preston.store;

import bio.guoda.preston.process.StatementsListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.rdf.api.BlankNode;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.Quad;

import java.io.IOException;

import static bio.guoda.preston.RefNodeFactory.getVersion;


public abstract class VersionProcessor extends StatementProcessor {
    private static Logger LOG = LoggerFactory.getLogger(VersionProcessor.class);

    public VersionProcessor(StatementsListener... listener) {
        super(listener);
    }

    @Override
    public void on(Quad statement) {
        try {
            BlankNodeOrIRI version = getVersion(statement);
            if (version instanceof BlankNode) {
                handleBlankVersion(statement, (BlankNode) version);
            } else {
                emit(statement);

            }
        } catch (Throwable e) {
            LOG.warn("failed to handle [" + statement.toString() + "]", e);
        }

    }

    abstract void handleBlankVersion(Quad statement, BlankNode blankVersion) throws IOException;

}
