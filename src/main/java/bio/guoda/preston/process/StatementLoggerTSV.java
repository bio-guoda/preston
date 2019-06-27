package bio.guoda.preston.process;

import bio.guoda.preston.cmd.LogErrorHandler;
import bio.guoda.preston.cmd.ReplayUtil;
import org.apache.commons.rdf.api.Triple;
import bio.guoda.preston.RDFUtil;

import java.io.PrintStream;

public class StatementLoggerTSV implements StatementListener {

    private final PrintStream out;
    private final LogErrorHandler error;

    public StatementLoggerTSV(PrintStream printWriter) {
        this(printWriter, new LogErrorHandler() {
            @Override
            public void handleError() {
                //ignore
            }
        });
    }

    public StatementLoggerTSV(PrintStream printWriter, LogErrorHandler errorHandler) {
        this.out = printWriter;
        this.error = errorHandler;
    }

    @Override
    public void on(Triple statement) {
        ReplayUtil.checkAndHandle(out, error);
        String subject = RDFUtil.getValueFor(statement.getSubject());
        String predicate = RDFUtil.getValueFor(statement.getPredicate());
        String object = RDFUtil.getValueFor(statement.getObject());
        out.println(subject + "\t" + predicate + "\t" + object);
    }

}
