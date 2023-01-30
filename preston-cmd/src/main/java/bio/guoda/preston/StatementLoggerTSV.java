package bio.guoda.preston;

import bio.guoda.preston.process.LogErrorHandler;
import bio.guoda.preston.process.StatementLogger;
import bio.guoda.preston.rdf.RDFUtil;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.Quad;

import java.io.OutputStream;
import java.util.Optional;

public class StatementLoggerTSV extends StatementLogger {

    public StatementLoggerTSV(OutputStream os) {
        super(os);
    }

    public StatementLoggerTSV(OutputStream os, LogErrorHandler handler) {
        super(os, handler);
    }

    @Override
    public void on(Quad statement) {
        String subject = RDFUtil.getValueFor(statement.getSubject());
        String predicate = RDFUtil.getValueFor(statement.getPredicate());
        String object = RDFUtil.getValueFor(statement.getObject());
        Optional<BlankNodeOrIRI> blankNodeOrIRI = statement.getGraphName();

        String graphName = blankNodeOrIRI.isPresent()
                ? RDFUtil.getValueFor(blankNodeOrIRI.get())
                : "";

        print(subject + "\t" + predicate + "\t" + object + "\t" + graphName + "\n");
    }

}
