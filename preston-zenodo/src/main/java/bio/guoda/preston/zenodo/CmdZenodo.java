package bio.guoda.preston.zenodo;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.StatementLogFactory;
import bio.guoda.preston.cmd.BlobStoreUtil;
import bio.guoda.preston.cmd.LogErrorHandlerExitOnError;
import bio.guoda.preston.process.EmittingStreamOfAnyVersions;
import bio.guoda.preston.process.StatementEmitter;
import bio.guoda.preston.process.StatementsEmitterAdapter;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.ValidatingKeyValueStreamContentAddressedFactory;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.commons.rdf.api.Quad;
import picocli.CommandLine;

import java.util.ArrayList;
import java.util.Collection;

@CommandLine.Command(
        name = "zenodo",
        description = "createEmptyDeposit/update associated Zenodo records"
)
public class CmdZenodo extends CmdZenodoEnabled implements Runnable {


    public static final int MAX_ZENODO_FILE_ATTACHMENTS = 100;

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
                LogErrorHandlerExitOnError.EXIT_ON_ERROR
        );

        // limit queue depth, but allow for maximum + 1
        Collection<Quad> fileDepositCandidates = new CircularFifoQueue<Quad>(MAX_ZENODO_FILE_ATTACHMENTS + 1);

        StatementsListener textMatcher = new ZenodoMetadataFileExtractor(
                this,
                blobStoreReadOnly,
                getZenodoContext(),
                fileDepositCandidates,
                listener
        );

        StatementEmitter emitter = new StatementsEmitterAdapter() {

            @Override
            public void emit(Quad statement) {
                if (!RefNodeFactory.isBlankOrSkolemizedBlank(statement.getSubject())) {

                    fileDepositCandidates.add(statement);
                }
                textMatcher.on(statement);
            }
        };

        new EmittingStreamOfAnyVersions(emitter, this)
                .parseAndEmit(getInputStream());

    }

}
