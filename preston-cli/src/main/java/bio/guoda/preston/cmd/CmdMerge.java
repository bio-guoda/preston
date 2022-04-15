package bio.guoda.preston.cmd;

import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.Archiver;
import bio.guoda.preston.store.BlobStore;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import picocli.CommandLine;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;

import static bio.guoda.preston.RefNodeConstants.USED_BY;
import static bio.guoda.preston.RefNodeFactory.toBlank;
import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toStatement;

@Parameters(separators = "= ", commandDescription = CmdMerge.MERGES_BIODIVERSITY_DATASET_GRAPHS)

@CommandLine.Command(
        name = "merge",
        aliases = {"join", "use", "import"},
        description = CmdMerge.MERGES_BIODIVERSITY_DATASET_GRAPHS
)
public class CmdMerge extends CmdActivity {

    public static final String MERGES_BIODIVERSITY_DATASET_GRAPHS = "Merges biodiversity dataset graphs";
    public static final String CONTENT_IDS_OF_PROVENANCE_GRAPHS_HASH_HASH = "Content ids of provenance graphs [hash://...] [hash://...] ...";
    @Parameter(description = CONTENT_IDS_OF_PROVENANCE_GRAPHS_HASH_HASH,
            validateWith = IRIValidator.class,
            converter = IRIConverter.class)
    @CommandLine.Parameters(
            description = CONTENT_IDS_OF_PROVENANCE_GRAPHS_HASH_HASH
    )
    private List<IRI> IRIs = new ArrayList<>();


    @Override
    void initQueue(Queue<List<Quad>> statementQueue, ActivityContext ctx) {

        List<Quad> mergingStatements = IRIs
                .stream()
                .map(iri -> toStatement(ctx.getActivity(), iri, USED_BY, ctx.getActivity()))
                .collect(Collectors.toList());

        statementQueue.add(mergingStatements);
    }

    @Override
    void processQueue(Queue<List<Quad>> statementQueue,
                      BlobStore blobStore,
                      ActivityContext ctx,
                      StatementsListener[] listeners) {
        while (!statementQueue.isEmpty()) {
            List<Quad> polled = statementQueue.poll();
            for (StatementsListener listener : listeners) {
                listener.on(polled);
            }
        }
    }

    @Override
    String getActivityDescription() {
        return "An event that merges biodiversity archives.";
    }


}
