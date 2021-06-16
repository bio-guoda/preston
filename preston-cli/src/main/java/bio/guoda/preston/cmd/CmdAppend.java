package bio.guoda.preston.cmd;

import bio.guoda.preston.process.EmittingStreamRDF;
import bio.guoda.preston.process.StatementsEmitter;
import bio.guoda.preston.process.StatementsEmitterAdapter;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.BlobStore;
import com.beust.jcommander.Parameters;
import org.apache.commons.rdf.api.BlankNode;
import org.apache.commons.rdf.api.Quad;

import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Command to (re-) process biodiversity dataset graph by providing some existing provenance logs.
 * <p>
 * Only considers already tracked datasets and their provenance.
 * <p>
 * See https://github.com/bio-guoda/preston/issues/15 .
 */

@Parameters(separators = "= ", commandDescription = "offline (re-)processing of tracked biodiversity dataset graph using stdin")
public class CmdAppend extends CmdActivity {

    private InputStream is = System.in;

    @Override
    void initQueue(Queue<List<Quad>> statementQueue, ActivityContext ctx) {
        // no initial seeds
    }

    @Override
    void processQueue(Queue<List<Quad>> statementQueue,
                      BlobStore blobStore,
                      ActivityContext ctx,
                      StatementsListener[] listeners) {
        handleQueuedMessages(statementQueue, listeners);

        StatementsEmitter emitter = new StatementsEmitterAdapter() {
            @Override
            public void emit(Quad statement) {
                emit(Collections.singletonList(statement));
            }

            @Override
            public void emit(List<Quad> statements) {
                handleNonBlankMessages(statements, listeners);
                handleQueuedMessages(statementQueue, listeners);
            }
        };
        new EmittingStreamRDF(emitter, this)
                .parseAndEmit(getInputStream());

    }

    @Override
    String getActivityDescription() {
        return "An event that (re-) processes existing biodiversity datasets graphs and their provenance.";
    }

    private void handleQueuedMessages(Queue<List<Quad>> statementQueue1, StatementsListener[] listeners) {
        while (!statementQueue1.isEmpty()) {
            List<Quad> polled = statementQueue1.poll();
            handleNonBlankMessages(polled, listeners);
        }
    }

    private void handleNonBlankMessages(List<Quad> statements, StatementsListener[] listeners) {
        List<Quad> nonBlankStatements = nonBlankStatements(statements);
        for (StatementsListener listener : listeners) {
            listener.on(nonBlankStatements);
        }
    }

    private List<Quad> nonBlankStatements(List<Quad> polled) {
        Stream<Quad> quadStream = polled.stream().filter(statement ->
                (!(statement.getSubject() instanceof BlankNode) && !(statement.getObject() instanceof BlankNode)));

        return quadStream.collect(Collectors.toList());
    }

    public void setInputStream(InputStream inputStream) {
        this.is = inputStream;
    }

    private InputStream getInputStream() {
        return(this.is);
    }
}
