package bio.guoda.preston.cmd;

import bio.guoda.preston.StatementLogFactory;
import bio.guoda.preston.process.EmittingStreamOfAnyVersions;
import bio.guoda.preston.process.StatementsEmitterAdapter;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.ValidatingKeyValueStreamContentAddressedFactory;
import bio.guoda.preston.zenodo.CmdZenodoEnabled;
import org.apache.commons.io.output.NullPrintStream;
import org.apache.commons.rdf.api.Quad;
import picocli.CommandLine;

import java.util.ArrayList;
import java.util.List;

@CommandLine.Command(
        name = "darktaxon-stream",
        description = "Stream DarkTaxon records into line-json with Zenodo metadata",
        hidden = true
)
public class CmdDarkTaxonStream extends CmdZenodoEnabled implements Runnable {

    @Override
    public void run() {
        BlobStoreReadOnly blobStoreAppendOnly
                = new BlobStoreAppendOnly(getKeyValueStore(new ValidatingKeyValueStreamContentAddressedFactory()), true, getHashType());
        run(BlobStoreUtil.createResolvingBlobStoreFor(blobStoreAppendOnly, this));

    }

    public void run(BlobStoreReadOnly blobStoreReadOnly) {
        StatementsListener listener = StatementLogFactory.createPrintingLogger(
                getLogMode(),
                NullPrintStream.INSTANCE,
                LogErrorHandlerExitOnError.EXIT_ON_ERROR);

        StatementsListener textMatcher = new DarkTaxonFileExtractor(
                this,
                blobStoreReadOnly,
                getOutputStream(),
                getZenodoContext(),
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

