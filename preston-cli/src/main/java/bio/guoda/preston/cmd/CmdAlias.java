package bio.guoda.preston.cmd;

import bio.guoda.preston.IRIFixingProcessor;
import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.StatementLogFactory;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.BlobStore;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.function.Predicate;

@Parameters(separators = "= ", commandDescription = "(friendly) aliases, or names, for content hashes")
public class CmdAlias extends CmdAppend implements Runnable {

    @Parameter(description = "[alias] [content hash] (e.g., [birds] [hash://sha256/123...])",
            validateWith = IRIValidator.class,
            converter = IRIConverter.class)
    private List<IRI> params = new ArrayList<>();

    @Override
    public void run() {

        final StatementsListener listener = StatementLogFactory
                .createPrintingLogger(getLogMode(), System.out, () -> System.exit(0));

        run(listener);

    }

    void run(StatementsListener listener) {
        if (params.size() < 2) {
            findSelectedAlias(listener);
        } else {
            appendAliasToProvenanceLog();
        }
    }

    private void appendAliasToProvenanceLog() {
        super.run();
    }

    private void findSelectedAlias(StatementsListener listener) {
        AliasUtil.findSelectedAlias(
                listener,
                selectorForParams(params),
                this
        );
    }

    private Predicate<Quad> selectorForParams(List<IRI> params) {
        Predicate<Quad> selector = quad -> RefNodeConstants.HAS_VERSION.equals(quad.getPredicate());

        if (params.size() > 0) {
            final IRI fixedIRI = new IRIFixingProcessor()
                    .process(this.params.get(0));
            selector = selector
                    .and(quad -> fixedIRI.equals(quad.getSubject()));
        }
        return selector;
    }


    @Override
    void initQueue(Queue<List<Quad>> statementQueue, ActivityContext ctx) {
        statementQueue.add(Collections
                .singletonList(RefNodeFactory.toStatement(
                        ctx.getActivity(),
                        params.get(0),
                        RefNodeConstants.HAS_VERSION,
                        params.get(1)))
        );
    }

    @Override
    void processQueue(Queue<List<Quad>> statementQueue,
                      BlobStore blobStore,
                      ActivityContext ctx,
                      StatementsListener[] listeners) {
        handleQueuedMessages(statementQueue, listeners);
    }


    @Override
    String getActivityDescription() {
        return "An activity that assigns an alias to a content hash";
    }

    void setParams(List<IRI> params) {
        this.params = params;
    }

}
