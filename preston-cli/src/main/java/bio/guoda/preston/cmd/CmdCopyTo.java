package bio.guoda.preston.cmd;

import bio.guoda.preston.process.EmittingStreamFactory;
import bio.guoda.preston.process.EmittingStreamOfAnyQuad;
import bio.guoda.preston.process.ParsingEmitter;
import bio.guoda.preston.process.ProcessorState;
import bio.guoda.preston.process.StatementEmitter;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.KeyGeneratingStream;
import bio.guoda.preston.store.KeyTo1LevelPath;
import bio.guoda.preston.store.KeyTo3LevelPath;
import bio.guoda.preston.store.KeyToPath;
import bio.guoda.preston.store.KeyValueStore;
import bio.guoda.preston.store.KeyValueStoreCopying;
import bio.guoda.preston.store.KeyValueStoreLocalFileSystem;
import bio.guoda.preston.store.ProvenanceTracer;
import bio.guoda.preston.store.ValidatingKeyValueStreamContentAddressedFactory;
import bio.guoda.preston.store.ValidatingKeyValueStreamHashTypeIRIFactory;
import bio.guoda.preston.util.JekyllUtil;
import org.apache.commons.collections4.Factory;
import org.apache.commons.rdf.api.IRI;
import org.joda.time.DateTime;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicReference;

import static bio.guoda.preston.RefNodeConstants.BIODIVERSITY_DATASET_GRAPH;
import static bio.guoda.preston.cmd.ReplayUtil.attemptReplay;


@CommandLine.Command(
        name = "cp",
        aliases = {"copyTo", "export"},
        description = "Copy biodiversity dataset graph"
)
public class CmdCopyTo extends LoggingPersisting implements Runnable {

    @CommandLine.Parameters(
            description = "Target directory"
    )
    private String targetDir;

    @CommandLine.Option(
            names = {"-t", "--type"},
            description = "Archive type. Supported values: ${COMPLETION-CANDIDATES}."
    )
    private ArchiveType archiveType = ArchiveType.data_prov_provindex;

    @CommandLine.Option(
            names = {"-p", "--target-hash-path-pattern"},
            description = "Hash path pattern of content to be copied. Supported values: ${COMPLETION-CANDIDATES}."
    )

    private HashPathPattern pathPattern = HashPathPattern.directoryDepth2;

    protected ArchiveType getArchiveType() {
        return archiveType;
    }


    @Override
    public void run() {
        File source = new File(getDataDir());
        File target = Persisting.mkdir(targetDir);
        if (ArchiveType.jekyll.equals(getArchiveType())) {
            generateJekyllSiteContent(target, getProvenanceTracer());
        } else {
            if (source.equals(target)) {
                throw new IllegalArgumentException("source dir [" + source.getAbsolutePath() + "] must be different from target dir [" + target.getAbsolutePath() + "].");
            }
            File tmp = new File(getTmpDir());

            Factory<KeyValueStore> keyValueStoreFactory = new Factory<KeyValueStore>() {
                @Override
                public KeyValueStore create() {
                    return getCopyingKeyValueStore(target, tmp);
                }
            };
            ProvenanceTracer tracer = getTracerOfDescendants(keyValueStoreFactory);

            if (ArchiveType.data_prov_provindex.equals(getArchiveType())) {
                copyAll(target, tmp, tracer);
            } else if (ArchiveType.data.equals(getArchiveType())) {
                copyDataOnly(target, tmp, tracer);
            } else if (ArchiveType.prov.equals(getArchiveType())) {
                copyProvLogsOnly(target, tmp, tracer);
            } else if (ArchiveType.provindex.equals(getArchiveType())) {
                copyProvIndexOnly(tracer);
            } else {
                throw new IllegalStateException("unsupport archive type [" + getArchiveType().name() + "]");
            }
        }

    }

    private void generateJekyllSiteContent(File target, ProvenanceTracer provenanceTracer) {
        final BlobStoreAppendOnly provenanceLogStore
                = new BlobStoreAppendOnly(getKeyValueStore(new ValidatingKeyValueStreamContentAddressedFactory()),
                true,
                getHashType()
        );

        final StatementsListener listener;
        try {
            listener = JekyllUtil.createJekyllSiteGenerator(provenanceLogStore, target);
        } catch (IOException e) {
            throw new RuntimeException("failed to create jekyll site content", e);
        }

        final AtomicReference<DateTime> lastCrawlTime = new AtomicReference<>();

        final CmdContext ctx = new CmdContext(
                this,
                getProvenanceAnchor(),
                listener,
                JekyllUtil.createPrestonStartTimeListener(lastCrawlTime::set)
        );

        attemptReplay(
                provenanceLogStore,
                ctx,
                provenanceTracer, new EmittingStreamFactory() {
                    @Override
                    public ParsingEmitter createEmitter(StatementEmitter emitter, ProcessorState context) {
                        return new EmittingStreamOfAnyQuad(emitter, context);
                    }
                }
        );

        JekyllUtil.writePrestonConfigFile(
                target,
                lastCrawlTime,
                getProvenanceAnchor(),
                provenanceTracer
        );
    }

    private void copyAll(File target, File tmp, ProvenanceTracer provenanceTracer) {
        KeyValueStore copyingKeyValueStore = new KeyValueStoreCopying(
                getKeyValueStore(new ValidatingKeyValueStreamContentAddressedFactory()),
                new KeyValueStoreLocalFileSystem(tmp, getKeyToPath(target),
                        new ValidatingKeyValueStreamContentAddressedFactory()));

        CloneUtil.clone(copyingKeyValueStore,
                copyingKeyValueStore,
                getHashType(),
                provenanceTracer, BIODIVERSITY_DATASET_GRAPH);
    }

    private KeyToPath getKeyToPath(File target) {
        if (HashPathPattern.directoryDepth0.equals(getPathPattern())) {
            return new KeyTo1LevelPath(target.toURI());
        } else {
            return new KeyTo3LevelPath(target.toURI());
        }
    }

    private void copyProvIndexOnly(ProvenanceTracer provenanceTracer) {

        CloneUtil.clone(
                new NullKeyValueStore(),
                getKeyValueStore(new ValidatingKeyValueStreamHashTypeIRIFactory()),
                getHashType(),
                provenanceTracer, BIODIVERSITY_DATASET_GRAPH
        );
    }

    private KeyValueStore getCopyingKeyValueStore(File target, File tmp) {
        return new KeyValueStoreCopying(
                getKeyValueStore(new ValidatingKeyValueStreamHashTypeIRIFactory()),
                new KeyValueStoreLocalFileSystem(tmp, getKeyToPath(target),
                        new ValidatingKeyValueStreamHashTypeIRIFactory()));
    }

    private void copyProvLogsOnly(File target, File tmp, ProvenanceTracer provenanceTracer) {
        KeyValueStore copyingKeyValueStoreProv = new KeyValueStoreCopying(
                getKeyValueStore(new ValidatingKeyValueStreamContentAddressedFactory()),
                new KeyValueStoreLocalFileSystem(tmp, getKeyToPath(target), new ValidatingKeyValueStreamContentAddressedFactory()));

        CloneUtil.clone(
                new NullKeyValueStore(),
                copyingKeyValueStoreProv,
                getHashType(),
                provenanceTracer, BIODIVERSITY_DATASET_GRAPH
        );
    }

    private void copyDataOnly(File target, File tmp, ProvenanceTracer provenanceTracer) {
        KeyValueStore copyingKeyValueStoreBlob = new KeyValueStoreCopying(
                getKeyValueStore(new ValidatingKeyValueStreamContentAddressedFactory()),
                new KeyValueStoreLocalFileSystem(tmp,
                        getKeyToPath(target),
                        new ValidatingKeyValueStreamContentAddressedFactory()));

        CloneUtil.clone(
                copyingKeyValueStoreBlob,
                getKeyValueStore(new ValidatingKeyValueStreamContentAddressedFactory()),
                getHashType(),
                provenanceTracer, BIODIVERSITY_DATASET_GRAPH);
    }

    private static class NullKeyValueStore implements KeyValueStore {

        @Override
        public IRI put(KeyGeneratingStream keyGeneratingStream, InputStream is) throws IOException {
            return null;
        }

        @Override
        public void put(IRI key, InputStream is) throws IOException {

        }

        @Override
        public InputStream get(IRI key) throws IOException {
            return null;
        }
    }

    public void setTargetDir(String targetDir) {
        this.targetDir = targetDir;
    }

    public void setArchiveType(ArchiveType archiveType) {
        this.archiveType = archiveType;
    }

    public void setPathPattern(HashPathPattern pathPattern) {
        this.pathPattern = pathPattern;
    }

    public HashPathPattern getPathPattern() {
        return pathPattern;
    }

}
