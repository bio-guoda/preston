package bio.guoda.preston.cmd;

import bio.guoda.preston.IRIFixingProcessor;
import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.StatementLogFactory;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.BlobStore;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.HashKeyUtil;
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

    @Parameter(description = "[alias] [content hash] (e.g., [birds.zip] [hash://sha256/123...])",
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
            IRI proposedAlias = params.get(0);
            if (HashKeyUtil.isLikelyCompositeHashURI(proposedAlias)) {
                throw new IllegalArgumentException("invalid alias: [" + proposedAlias.getIRIString() + "] is a (composite) hash uris cannot be used as alias");
            }
            IRI contentHash = params.get(1);
            if (!HashKeyUtil.isLikelyCompositeHashURI(contentHash)) {
                throw new IllegalArgumentException("invalid target: alias [" + proposedAlias.getIRIString() + "] points to invalid hash uri [" + contentHash.getIRIString() + "]");
            }

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
            final IRI origIRI = this.params.get(0);
            final IRI fixedIRI = new IRIFixingProcessor()
                    .process(origIRI);
            selector = selector
                    .and(quad -> origIRI.equals(quad.getSubject()) || fixedIRI.equals(quad.getSubject()));
        }
        return selector;
    }


    @Override
    void initQueue(Queue<List<Quad>> statementQueue, ActivityContext ctx) {
        statementQueue.add(Collections
                .singletonList(RefNodeFactory.toStatement(
                        ctx.getActivity(),
                        new IRIFixingProcessor()
                                .process(params.get(0)),
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
    protected StatementsListener[] initListeners(BlobStoreReadOnly blobStore, StatementsListener archivingLogger, Queue<List<Quad>> statementQueue) {
        return new StatementsListener[]{
                archivingLogger,
                StatementLogFactory.createPrintingLogger(getLogMode(), System.out, this)
        };
    }


    @Override
    String getActivityDescription() {
        return "An activity that assigns an alias to a content hash";
    }

    void setParams(List<IRI> params) {
        this.params = params;
    }

}
