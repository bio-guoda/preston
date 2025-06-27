package bio.guoda.preston.cmd;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.process.RegistryReaderALA;
import bio.guoda.preston.process.RegistryReaderBHL;
import bio.guoda.preston.process.RegistryReaderBioCASE;
import bio.guoda.preston.process.RegistryReaderChecklistBank;
import bio.guoda.preston.process.RegistryReaderDOI;
import bio.guoda.preston.process.RegistryReaderDataONE;
import bio.guoda.preston.process.RegistryReaderGBIF;
import bio.guoda.preston.process.RegistryReaderGitHubIssues;
import bio.guoda.preston.process.RegistryReaderGoogleDrive;
import bio.guoda.preston.process.RegistryReaderIDigBio;
import bio.guoda.preston.process.RegistryReaderOAI;
import bio.guoda.preston.process.RegistryReaderOBIS;
import bio.guoda.preston.process.RegistryReaderRSS;
import bio.guoda.preston.process.RegistryReaderTaxonWorks;
import bio.guoda.preston.process.RegistryReaderZotero;
import bio.guoda.preston.process.SciELOSoftRedirector;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.Dereferencer;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import picocli.CommandLine;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeConstants.WAS_ASSOCIATED_WITH;
import static bio.guoda.preston.RefNodeFactory.toBlank;
import static bio.guoda.preston.RefNodeFactory.toStatement;

@CommandLine.Command(
        name = "track",
        header = "track content at resource location(s) or from stdin",
        aliases = {"update"},
        description = "Track content at some resource location (or url)",
        footerHeading = "Examples",
        footer = {
                "%n1.",
                "Track a natural history collection at UC Santa Barbara UCSB-IZC via their GBIF Dataset DOI, then print their first record.",
                "----",
                "preston track\\%n" +
                        " https://doi.org/10.15468/w6hvhv\\%n" +
                        " | preston dwc-stream\\%n" +
                        " | head -1\\%n" +
                        " | jq .\\%n" +
                        " > specimen.json",
                "----",
                "%n2.",
                "Get a DwC-A file, track that DwC-A file via stdin, then print their first record into specimen.json.",
                "----",
                "preston cat\\%n" +
                        " --remote https://softwareheritage.org\\%n" +
                        " hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1\\%n" +
                        " > dwca.zip",
                "cat dwca.zip\\%n" +
                        " | preston track\\%n" +
                        " | preston dwc-stream\\%n" +
                        " | head -1\\%n" +
                        " | jq .\\%n" +
                        " > specimen.json",
                "----",
                "%n3.",
                "Track content locations in files, then print their first record.",
                "----",
                "echo https://en.wikipedia.org/wiki/Cat > urls.txt%n" +
                "echo https://en.wikipedia.org/wiki/Dog >> urls.txt%n" +
                "preston track -f urls.txt\\%n" +
                        " | preston cat\\%n" +
                        " > cat-and-dog.html",
                "----"
        }
)

public class CmdUpdate extends CmdTrack {

    @CommandLine.Option(
            names = {"-u", "--seed"},
            description = "Starting points for graph discovery. Only active when no content urls are provided."
    )
    private List<IRI> seeds = new ArrayList<>();

    @Override
    void initQueue(Queue<List<Quad>> statementQueue, ActivityContext ctx) {
        if (shouldListenToStdIn()) {
            UUID uuid = UUID.randomUUID();
            statementQueue.add(Collections.singletonList(
                    toStatement(ctx.getActivity(),
                            RefNodeFactory.toIRI(uuid),
                            HAS_VERSION,
                            toBlank()
                    )
            ));
            setDereferencer(getDereferencerOfInputStream(uuid, getInputStream()));
        } else if (getIRIs().isEmpty()) {
            statementQueue.add(generateSeeds(ctx.getActivity()));
        } else {
            getIRIs().forEach(iri -> {
                statementQueue.add(Collections.singletonList(
                        toStatement(
                                ctx.getActivity(),
                                iri,
                                HAS_VERSION,
                                toBlank()
                        )
                ));
            });
        }
    }

    private boolean shouldListenToStdIn() {
        return getIRIs().isEmpty() && seeds.isEmpty() && StringUtils.isBlank(getFilename());
    }

    private Dereferencer<InputStream> getDereferencerOfInputStream(UUID uuid, final InputStream inputStream) {
        return uri -> {
            if (!RefNodeFactory.toIRI(uuid).equals(uri)) {
                throw new IOException("failed to dereference content: iri [" + uri + "] is not associated with any content stream");
            }
            return inputStream;
        };
    }

    @Override
    public String getDescriptionDefault() {
        return "A crawl event that discovers biodiversity archives.";
    }

    @Override
    protected Stream<StatementsListener> createProcessors(BlobStoreReadOnly blobStore, StatementsListener queueAsListener) {
        return Stream.of(
                new RegistryReaderALA(blobStore, queueAsListener),
                new RegistryReaderIDigBio(blobStore, queueAsListener),
                new RegistryReaderBHL(blobStore, queueAsListener),
                new RegistryReaderBioCASE(blobStore, queueAsListener),
                new RegistryReaderChecklistBank(blobStore, queueAsListener),
                new RegistryReaderDataONE(blobStore, queueAsListener),
                new RegistryReaderDOI(blobStore, queueAsListener),
                new RegistryReaderGBIF(blobStore, queueAsListener),
                new RegistryReaderGitHubIssues(blobStore, queueAsListener),
                new RegistryReaderOBIS(blobStore, queueAsListener),
                new RegistryReaderRSS(blobStore, queueAsListener),
                new RegistryReaderTaxonWorks(blobStore, queueAsListener),
                new RegistryReaderZotero(blobStore, queueAsListener),
                new RegistryReaderGoogleDrive(blobStore, queueAsListener),
                new RegistryReaderOAI(blobStore, queueAsListener),
                new SciELOSoftRedirector(blobStore, queueAsListener)
        );
    }

    private List<Quad> generateSeeds(final IRI crawlActivity) {
        return seeds.stream()
                .map(seed -> toStatement(crawlActivity, seed, WAS_ASSOCIATED_WITH, crawlActivity))
                .collect(Collectors.toList());
    }

    public void setSeeds(List<IRI> seeds) {
        this.seeds = seeds;
    }

    public List<IRI> getSeeds() {
        return seeds;
    }
}
