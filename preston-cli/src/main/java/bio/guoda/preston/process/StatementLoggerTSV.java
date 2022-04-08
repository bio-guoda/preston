package bio.guoda.preston.process;

import bio.guoda.preston.RDFUtil;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.Quad;

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Optional;

public class StatementLoggerTSV extends StatementLogger {

    public StatementLoggerTSV(OutputStream printWriter) {
        super(printWriter);
    }

    public StatementLoggerTSV(OutputStream printWriter, LogErrorHandler handler) {
        super(printWriter, handler);
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
