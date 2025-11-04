package bio.guoda.preston.cmd;

import bio.guoda.preston.StatementLogFactory;
import bio.guoda.preston.process.EmittingStreamOfAnyVersions;
import bio.guoda.preston.process.StatementsEmitterAdapter;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.ValidatingKeyValueStreamContentAddressedFactory;
import bio.guoda.preston.stream.ContentHashDereferencer;
import org.apache.commons.io.output.NullPrintStream;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import picocli.CommandLine;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

@CommandLine.Command(
        name = "zotero2ris",
        aliases = {"zotero-to-ris-stream"},
        description = "Stream Zotero records into RIS records"
)
public class CmdZoteroToRISStream extends LoggingPersisting implements Runnable {

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


        StatementsListener textMatcher = new ZoteroFileExtractorRIS(
                this,
                blobStoreWithIndexedVersions,
                getOutputStream(),
                AnchorUtil.findAnchorOrThrow(this),
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

