package bio.guoda.preston.zenodo;

import bio.guoda.preston.EnvUtil;
import bio.guoda.preston.StatementLogFactory;
import bio.guoda.preston.cmd.BlobStoreUtil;
import bio.guoda.preston.cmd.LogErrorHandlerExitOnError;
import bio.guoda.preston.cmd.LoggingPersisting;
import bio.guoda.preston.process.EmittingStreamOfAnyVersions;
import bio.guoda.preston.process.StatementsEmitterAdapter;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.ValidatingKeyValueStreamContentAddressedFactory;
import org.apache.commons.rdf.api.Quad;
import picocli.CommandLine;

import java.util.Collections;
import java.util.List;

@CommandLine.Command(
        name = "zenodo",
        description = "create/update associated Zenodo records"
)
public class CmdZenodo extends CmdZenodoEnabled implements Runnable {


    @Override
    public void run() {
        BlobStoreReadOnly blobStoreAppendOnly
                = new BlobStoreAppendOnly(getKeyValueStore(new ValidatingKeyValueStreamContentAddressedFactory()),
                true, getHashType());
        run(BlobStoreUtil.createResolvingBlobStoreFor(blobStoreAppendOnly, this));

    }

    public void run(BlobStoreReadOnly blobStoreReadOnly) {
        StatementsListener listener = StatementLogFactory.createPrintingLogger(
                getLogMode(),
                getOutputStream(),
                LogErrorHandlerExitOnError.EXIT_ON_ERROR);

        StatementsListener textMatcher = new ZenodoMetadataFileExtractor(
                this,
                blobStoreReadOnly,
                getZenodoContext(),
                listener
        );

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
