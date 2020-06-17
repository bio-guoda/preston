package bio.guoda.preston.process;

import bio.guoda.preston.RDFUtil;
import bio.guoda.preston.model.RefNodeFactory;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.Quad;

import java.io.PrintStream;
import java.util.Optional;

public class StatementLoggerTSV extends StatementsListenerAdapter {

    private final PrintStream out;

    public StatementLoggerTSV(PrintStream printWriter) {
        this.out = printWriter;
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

        out.print(subject + "\t" + predicate + "\t" + object + "\t" + graphName + "\n");
    }

}
