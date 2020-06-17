package bio.guoda.preston.cmd;

import bio.guoda.preston.process.EmittingStreamRDF;
import bio.guoda.preston.process.StatementEmitter;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.BlobStore;
import com.beust.jcommander.Parameters;
import org.apache.commons.rdf.api.BlankNode;
import org.apache.commons.rdf.api.Quad;

import java.io.InputStream;
import java.util.Queue;

/**
 * Command to (re-) process biodiversity dataset graph by providing some existing provenance logs.
 * <p>
 * Only considers already tracked datasets and their provenance.
 * <p>
 * See https://github.com/bio-guoda/preston/issues/15 .
 */

@Parameters(separators = "= ", commandDescription = "offline (re-)processing of tracked biodiversity dataset graph using stdin")
public class CmdProcess extends CmdActivity {

    private InputStream is = System.in;

    @Override
    void initQueue(Queue<Quad> statementQueue, ActivityContext ctx) {
        // no initial seeds
    }

    @Override
    void processQueue(Queue<Quad> statementQueue,
                      BlobStore blobStore,
                      ActivityContext ctx,
                      StatementsListener[] listeners) {
        handleQueuedMessages(statementQueue, listeners);

        StatementEmitter emitter = statement -> {
            handleStatement(statement, listeners);
            handleQueuedMessages(statementQueue, listeners);
        };
        new EmittingStreamRDF(emitter, this).parseAndEmit(getInputStream());

    }

    @Override
    String getActivityDescription() {
        return "An event that (re-) processes existing biodiversity datasets graphs and their provenance.";
    }


    private void handleStatement(Quad statement, StatementsListener[] listeners) {
        if ((!(statement.getSubject() instanceof BlankNode) && !(statement.getObject() instanceof BlankNode))) {
            for (StatementsListener listener : listeners) {
                listener.on(statement);
            }
        }
    }

    private void handleQueuedMessages(Queue<Quad> statementQueue1, StatementsListener[] listeners) {
        while (!statementQueue1.isEmpty()) {
            Quad polled = statementQueue1.poll();
            handleStatement(polled, listeners);
        }
    }

    public void setInputStream(InputStream inputStream) {
        this.is = inputStream;
    }
    private InputStream getInputStream() {
        return(this.is);
    }
}
