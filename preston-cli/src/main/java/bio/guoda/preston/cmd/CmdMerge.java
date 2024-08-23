package bio.guoda.preston.cmd;

import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.BlobStore;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import picocli.CommandLine;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;

import static bio.guoda.preston.RefNodeConstants.USED_BY;
import static bio.guoda.preston.RefNodeFactory.toStatement;

@CommandLine.Command(
        name = "merge",
        aliases = {"join", "use", "import"},
        description = "Merges biodiversity dataset graphs"
)
public class CmdMerge extends CmdActivity {

    @CommandLine.Parameters(
            description = "Content ids of provenance graphs [hash://...] [hash://...] ..."
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
    public String getDescriptionDefault() {
        return "An event that merges biodiversity archives.";
    }


}
