package bio.guoda.preston.cmd;

import bio.guoda.preston.ArchiveUtil;
import bio.guoda.preston.StatementLogFactory;
import bio.guoda.preston.index.QuadIndex;
import bio.guoda.preston.index.QuadIndexImpl;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.HashKeyUtil;
import bio.guoda.preston.store.KeyTo1LevelPath;
import bio.guoda.preston.store.ValidatingKeyValueStreamContentAddressedFactory;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

@CommandLine.Command(
        name = "describe",
        description = "Searches indexed RDF for information about the specified identifier",
        hidden = true
)
public class CmdDescribe extends LoggingPersisting implements Runnable {
    private static final int MAX_HITS = 1000;

    @CommandLine.Option(names = {"-i", "--index"}, description = "The hash URI of the index to use (e.g. [hash://sha256/abc...])")
    private IRI indexVersion;

    @CommandLine.Parameters(description = "The identifiers used to query the index")
    private List<IRI> searchIris = new ArrayList<>();

    @Override
    public void run() {
        BlobStoreReadOnly blobStore = new BlobStoreAppendOnly(getKeyValueStore(new ValidatingKeyValueStreamContentAddressedFactory(getHashType())), true, getHashType());
        run(blobStore);
    }

    public void run(BlobStoreReadOnly blobStore) {
        StatementsListener listener = StatementLogFactory.createPrintingLogger(
                getLogMode(),
                getOutputStream(),
                LogErrorHandlerExitOnError.EXIT_ON_ERROR
        );

        File indexDir = extractIndexFromBlobstore(indexVersion, blobStore, getTmpDir());

        try (QuadIndex index = new QuadIndexImpl(indexDir)) {
            searchIris.forEach(iri -> findAndEmitMatches(index, iri, listener));
        } catch (IOException e) {
            throw new RuntimeException("failed to open index located at " + indexDir, e);
        }
    }

    private File extractIndexFromBlobstore(IRI indexVersion, BlobStoreReadOnly blobStore, File destination) {
        URI indexPathOnDisk = new KeyTo1LevelPath(destination.toURI(), HashKeyUtil.hashTypeFor(indexVersion))
                .toPath(indexVersion);

        File indexDir = new File(indexPathOnDisk);

        if (!indexDir.exists()) {
            try {
                ArchiveUtil.unpackTarGzFromBlobstore(blobStore, indexVersion, indexDir);
            } catch (IOException e) {
                throw new RuntimeException("failed to unpack index " + indexVersion + " to " + indexDir, e);
            }
        }

        return indexDir;
    }

    private static void findAndEmitMatches(QuadIndex index, BlankNodeOrIRI iri, StatementsListener listener) {
        try {
            Stream.concat(
                    index.findQuadsWithSubject(iri, MAX_HITS),
                    index.findQuadsWithObject(iri, MAX_HITS)
            ).forEach(listener::on);
        } catch (IOException e) {
            throw new RuntimeException("failed to read index", e);
        }
    }
}
