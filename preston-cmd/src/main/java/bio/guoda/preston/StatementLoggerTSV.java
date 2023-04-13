package bio.guoda.preston;

import bio.guoda.preston.process.LogErrorHandler;
import bio.guoda.preston.process.StatementLogger;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.Quad;

import java.io.OutputStream;
import java.util.Optional;
import java.util.function.Consumer;

public class StatementLoggerTSV extends StatementLogger {

    public StatementLoggerTSV(OutputStream os) {
        super(os);
    }

    public StatementLoggerTSV(OutputStream os, LogErrorHandler handler) {
        super(os, handler);
    }

    @Override
    public void on(Quad statement) {
        quadToTSV(statement, new Consumer<String>() {

            @Override
            public void accept(String s) {
                print(s);
            }
        });
    }

    public static void quadToTSV(Quad statement, Consumer<String> sink) {
        String subject = RDFUtil.getValueFor(statement.getSubject());
        String predicate = RDFUtil.getValueFor(statement.getPredicate());
        String object = RDFUtil.getValueFor(statement.getObject());
        Optional<BlankNodeOrIRI> blankNodeOrIRI = statement.getGraphName();

        String graphName = blankNodeOrIRI.isPresent()
                ? RDFUtil.getValueFor(blankNodeOrIRI.get())
                : "";

        sink.accept(subject + "\t" + predicate + "\t" + object + "\t" + graphName + "\n");
    }

}
