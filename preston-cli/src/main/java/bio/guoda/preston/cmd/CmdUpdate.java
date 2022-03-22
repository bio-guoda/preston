package bio.guoda.preston.cmd;

import bio.guoda.preston.ResourcesHTTP;
import bio.guoda.preston.Seeds;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.Archiver;
import bio.guoda.preston.store.BlobStore;
import bio.guoda.preston.store.DereferencerContentAddressed;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;

import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeConstants.WAS_ASSOCIATED_WITH;
import static bio.guoda.preston.RefNodeFactory.toBlank;
import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toStatement;

@Parameters(separators = "= ", commandDescription = "update biodiversity dataset graph")
public class CmdUpdate extends CmdActivity {

    @Parameter(names = {"-u", "--seed"}, description = "starting points for graph discovery. Only active when no content urls are provided.", validateWith = URIValidator.class)
    private List<String> seedUrls = new ArrayList<String>() {{
        add(Seeds.IDIGBIO.getIRIString());
        add(Seeds.GBIF.getIRIString());
        add(Seeds.BIOCASE.getIRIString());
    }};

    @Parameter(description = "[url1] [url2] ...",
            validateWith = IRIValidator.class)
    private List<String> IRIs = new ArrayList<>();


    @Override
    void initQueue(Queue<List<Quad>> statementQueue, ActivityContext ctx) {
        if (IRIs.isEmpty()) {
            statementQueue.add(generateSeeds(ctx.getActivity()));
        } else {
            IRIs.forEach(iri -> {
                Quad quad = toStatement(ctx.getActivity(), toIRI(iri), HAS_VERSION, toBlank());
                statementQueue.add(Collections.singletonList(quad));
            });
        }
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
        return "A crawl event that discovers biodiversity archives.";
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


    private List<Quad> generateSeeds(final IRI crawlActivity) {
        return seedUrls.stream()
                .map((String uriString) -> toStatement(crawlActivity, toIRI(uriString), WAS_ASSOCIATED_WITH, crawlActivity))
                .collect(Collectors.toList());
    }

}
