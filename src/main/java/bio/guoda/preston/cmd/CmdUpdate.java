package bio.guoda.preston.cmd;

import bio.guoda.preston.Resources;
import bio.guoda.preston.Seeds;
import bio.guoda.preston.process.StatementListener;
import bio.guoda.preston.store.Archiver;
import bio.guoda.preston.store.BlobStore;
import bio.guoda.preston.store.DereferencerContentAddressed;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Triple;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;

import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeConstants.WAS_ASSOCIATED_WITH;
import static bio.guoda.preston.model.RefNodeFactory.toBlank;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;

@Parameters(separators = "= ", commandDescription = "update biodiversity dataset graph")
public class CmdUpdate extends CmdActivity {

    @Parameter(names = {"-u", "--seed"}, description = "starting points for graph discovery. Only active when no content urls are provided.", validateWith = URIValidator.class)
    private List<String> seedUrls = new ArrayList<String>() {{
        add(Seeds.IDIGBIO.getIRIString());
        add(Seeds.GBIF.getIRIString());
        add(Seeds.BIOCASE.getIRIString());
        add(Seeds.OBIS.getIRIString());
        //add(Seeds.DATA_ONE.getIRIString());
    }};

    @Parameter(description = "[url1] [url2] ...",
            validateWith = IRIValidator.class)
    private List<String> IRIs = new ArrayList<>();


    @Override
    void initQueue(Queue<Triple> statementQueue, ActivityContext ctx) {
        if (IRIs.isEmpty()) {
            statementQueue.addAll(generateSeeds(ctx.getActivity()));
        } else {
            IRIs.forEach(iri -> statementQueue.add(toStatement(toIRI(iri), HAS_VERSION, toBlank())));
        }
    }

    @Override
    void processQueue(Queue<Triple> statementQueue, BlobStore blobStore, ActivityContext ctx, StatementListener[] listeners) {
        StatementListener processor = createActivityProcessor(blobStore, ctx, listeners);

        while (!statementQueue.isEmpty()) {
            processor.on(statementQueue.poll());
        }
    }

    @Override
    String getActivityDescription() {
        return "A crawl event that discovers biodiversity archives.";
    }


    private StatementListener createActivityProcessor(BlobStore blobStore, ActivityContext ctx, StatementListener[] listeners) {
        return new Archiver(
                new DereferencerContentAddressed(Resources::asInputStream, blobStore),
                ctx,
                listeners);
    }


    private List<Triple> generateSeeds(final IRI crawlActivity) {
        return seedUrls.stream()
                .map((String uriString) -> toStatement(toIRI(uriString), WAS_ASSOCIATED_WITH, crawlActivity))
                .collect(Collectors.toList());
    }

}
