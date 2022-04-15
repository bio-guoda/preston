package bio.guoda.preston.cmd;

import bio.guoda.preston.Seeds;
import bio.guoda.preston.process.RegistryReaderALA;
import bio.guoda.preston.process.RegistryReaderBHL;
import bio.guoda.preston.process.RegistryReaderBioCASE;
import bio.guoda.preston.process.RegistryReaderDOI;
import bio.guoda.preston.process.RegistryReaderDataONE;
import bio.guoda.preston.process.RegistryReaderGBIF;
import bio.guoda.preston.process.RegistryReaderIDigBio;
import bio.guoda.preston.process.RegistryReaderOBIS;
import bio.guoda.preston.process.RegistryReaderRSS;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.BlobStoreReadOnly;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import picocli.CommandLine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeConstants.WAS_ASSOCIATED_WITH;
import static bio.guoda.preston.RefNodeFactory.toBlank;
import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toStatement;

@CommandLine.Command(
        name = "update",
        aliases = {"track"},
        description = "Update biodiversity dataset graph"
)

public class CmdUpdate extends CmdTrack {

    @CommandLine.Option(
            names = {"-u", "--seed"},
            description = "Starting points for graph discovery. Only active when no content urls are provided."
    )
    private List<String> seedUrls = new ArrayList<String>() {{
        add(Seeds.IDIGBIO.getIRIString());
        add(Seeds.GBIF.getIRIString());
        add(Seeds.BIOCASE.getIRIString());
    }};

    @Override
    void initQueue(Queue<List<Quad>> statementQueue, ActivityContext ctx) {
        if (getIRIs().isEmpty()) {
            statementQueue.add(generateSeeds(ctx.getActivity()));
        } else {
            getIRIs().forEach(iri -> {
                Quad quad = toStatement(ctx.getActivity(), iri, HAS_VERSION, toBlank());
                statementQueue.add(Collections.singletonList(quad));
            });
        }
    }

    @Override
    String getActivityDescription() {
        return "A crawl event that discovers biodiversity archives.";
    }

    @Override
    protected Stream<StatementsListener> createProcessors(BlobStoreReadOnly blobStore, StatementsListener queueAsListener) {
        return Stream.of(
                new RegistryReaderIDigBio(blobStore, queueAsListener),
                new RegistryReaderGBIF(blobStore, queueAsListener),
                new RegistryReaderBioCASE(blobStore, queueAsListener),
                new RegistryReaderDataONE(blobStore, queueAsListener),
                new RegistryReaderRSS(blobStore, queueAsListener),
                new RegistryReaderBHL(blobStore, queueAsListener),
                new RegistryReaderOBIS(blobStore, queueAsListener),
                new RegistryReaderDOI(blobStore, queueAsListener),
                new RegistryReaderALA(blobStore, queueAsListener)
        );
    }

    private List<Quad> generateSeeds(final IRI crawlActivity) {
        return seedUrls.stream()
                .map((String uriString) -> toStatement(crawlActivity, toIRI(uriString), WAS_ASSOCIATED_WITH, crawlActivity))
                .collect(Collectors.toList());
    }

}
