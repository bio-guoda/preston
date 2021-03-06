package bio.guoda.preston.cmd;

import bio.guoda.preston.IRIFixingProcessor;
import bio.guoda.preston.StatementIRIProcessor;
import bio.guoda.preston.StatementLogFactory;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.KeyValueStoreLocalFileSystem;
import bio.guoda.preston.store.HexaStoreImpl;
import com.beust.jcommander.Parameters;

import static bio.guoda.preston.cmd.ReplayUtil.attemptReplay;

@Parameters(separators = "= ", commandDescription = "show biodiversity dataset provenance logs")
public class CmdList extends LoggingPersisting implements Runnable {

    @Override
    public void run() {
        run(() -> System.exit(0));
    }

    public void run(LogErrorHandler handler) {
        StatementsListener listener = StatementLogFactory.createPrintingLogger(getLogMode(), System.out, handler);

        StatementIRIProcessor processor = new StatementIRIProcessor(listener);
        processor.setIriProcessor(new IRIFixingProcessor());

        attemptReplay(
                new BlobStoreAppendOnly(getKeyValueStore(new KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory())),
                new HexaStoreImpl(getKeyValueStore(new KeyValueStoreLocalFileSystem.KeyValueStreamFactorySHA256Values())),
                new CmdContext(this, processor));
    }

}
