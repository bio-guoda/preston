package bio.guoda.preston.cmd;

import bio.guoda.preston.StatementLogFactory;
import bio.guoda.preston.process.EmittingStreamOfAnyVersions;
import bio.guoda.preston.process.StatementsEmitterAdapter;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.stream.ContentHashDereferencer;
import bio.guoda.preston.store.ValidatingKeyValueStreamContentAddressedFactory;
import org.apache.commons.io.output.NullPrintStream;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import picocli.CommandLine;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

@CommandLine.Command(
        name = "zotero-stream",
        description = "Stream Zotero records into line-json with Zenodo metadata"
)
public class CmdZoteroStream extends LoggingPersisting implements Runnable {


    @CommandLine.Option(
            names = {"--community", "--communities"},
            split = ",",
            description = "select which Zenodo communities to submit to. If community is known (e.g., batlit, taxodros), default metadata is included."
    )

    private List<String> communities = new ArrayList<>();

    @CommandLine.Option(
            names = {"--append-doi-to-title"},
            description = "append provided DOI to the Zenodo deposit record title. This way, the provided DOI appears along with the Zenodo record DOI in the Zenodo generated citation string. E.g., Pestana, L. B., Dias, G. M., & Marques, A. C. (2017). A century of introductions by coastal sessile marine invertebrates in Angola, South East Atlantic Ocean https://doi.org/10.1016/j.marpolbul.2017.09.041. Marine Pollution Bulletin, 125(1-2), 426–432. https://doi.org/10.5072/zenodo.418845"
    )
    private boolean appendProvidedDoiToTitle = false;


    @Override
    public void run() {
        BlobStoreReadOnly blobStoreAppendOnly
                = new BlobStoreAppendOnly(getKeyValueStore(new ValidatingKeyValueStreamContentAddressedFactory()), true, getHashType());
        BlobStoreReadOnly blobStoreReadOnly = new BlobStoreReadOnly() {

            private ContentHashDereferencer contentHashDereferencer = new ContentHashDereferencer(blobStoreAppendOnly);

            @Override
            public InputStream get(IRI uri) throws IOException {
                return contentHashDereferencer.get(uri);
            }
        };
        run(blobStoreReadOnly);

    }


    public void run(BlobStoreReadOnly blobStoreReadOnly) {

        BlobStoreReadOnly blobStoreWithIndexedVersions = BlobStoreUtil.createIndexedBlobStoreFor(blobStoreReadOnly, BlobStoreUtil.contentForAlias(this));


        StatementsListener listener = StatementLogFactory.createPrintingLogger(
                getLogMode(),
                NullPrintStream.INSTANCE,
                LogErrorHandlerExitOnError.EXIT_ON_ERROR);

        StatementsListener textMatcher = new ZoteroFileExtractorZenodo(
                this,
                blobStoreWithIndexedVersions,
                getOutputStream(),
                communities,
                AnchorUtil.findAnchorOrThrow(this),
                appendProvidedDoiToTitle,
                listener);

        StatementsEmitterAdapter emitter = new StatementsEmitterAdapter() {

            @Override
            public void emit(Quad statement) {
                textMatcher.on(statement);
            }
        };

        new EmittingStreamOfAnyVersions(emitter, this)
                .parseAndEmit(getInputStream());

    }

}

