package bio.guoda.preston.cmd;

import bio.guoda.preston.ResourcesHTTP;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.Archiver;
import bio.guoda.preston.store.BlobStore;
import bio.guoda.preston.store.DereferencerContentAddressed;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.commons.rdf.api.Quad;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;

import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeFactory.toBlank;
import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toStatement;

@Parameters(separators = "= ", commandDescription = "tracks resources via their IRIs")
public class CmdTrack extends CmdActivity {

    @Parameter(description = "[url1] [url2] ...",
            validateWith = IRIValidator.class)
    private List<String> IRIs = new ArrayList<>();

    @Override
    void initQueue(Queue<List<Quad>> statementQueue, ActivityContext ctx) {
        IRIs.forEach(iri -> {
            Quad quad = toStatement(ctx.getActivity(), toIRI(iri), HAS_VERSION, toBlank());
            statementQueue.add(Collections.singletonList(quad));
        });
    }

    @Override
    void processQueue(Queue<List<Quad>> statementQueue,
                      BlobStore blobStore,
                      ActivityContext ctx,
                      StatementsListener[] listeners) {
        StatementsListener processor = createActivityProcessor(blobStore, ctx, listeners);

        while (!statementQueue.isEmpty()) {
            processor.on(statementQueue.poll());
        }
    }

    @Override
    String getActivityDescription() {
        return "A crawl event that tracks digital content.";
    }

    private StatementsListener createActivityProcessor(
            BlobStore blobStore,
            ActivityContext ctx,
            StatementsListener[] listeners) {
        return new Archiver(
                new DereferencerContentAddressed(ResourcesHTTP::asInputStream, blobStore),
                ctx,
                listeners);
    }

}
