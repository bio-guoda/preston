package bio.guoda.preston.cmd;

import bio.guoda.preston.StatementLogFactory;
import bio.guoda.preston.process.EmittingStreamRDF;
import bio.guoda.preston.process.DwcRecordExtractor;
import bio.guoda.preston.process.StatementsEmitterAdapter;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.KeyValueStoreLocalFileSystem;
import com.beust.jcommander.Parameters;
import org.apache.commons.io.output.NullPrintStream;
import org.apache.commons.rdf.api.Quad;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;

@Parameters(separators = "= ", commandDescription = "Extract records from DarwinCore archives in line-json")
public class CmdDwcRecordStream extends LoggingPersisting implements Runnable {

    private InputStream inputStream = System.in;
    private OutputStream out = System.out;

    @Override
    public void run() {
        BlobStoreReadOnly blobStoreAppendOnly
                = new BlobStoreAppendOnly(
                        getKeyValueStore(new KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory(getHashType()))
        );
        run(resolvingBlobStore(blobStoreAppendOnly));

    }

    public void run(BlobStoreReadOnly blobStoreReadOnly) {
        StatementsListener listener = StatementLogFactory.createPrintingLogger(
                getLogMode(),
                new NullPrintStream(),
                () -> System.exit(0));

        out = System.out;
        DwcRecordExtractor textMatcher = new DwcRecordExtractor(
                this,
                blobStoreReadOnly,
                out,
                listener);

        StatementsEmitterAdapter emitter = new StatementsEmitterAdapter() {

            @Override
            public void emit(Quad statement) {
                textMatcher.on(statement);
            }
        };

        new EmittingStreamRDF(emitter, this)
                .parseAndEmit(inputStream);

    }

    public void setInputStream(InputStream inputStream) {
        this.inputStream = inputStream;
    }

}

