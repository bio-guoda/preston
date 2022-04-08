package bio.guoda.preston.cmd;

import bio.guoda.preston.StatementLogFactory;
import bio.guoda.preston.process.CitationGenerator;
import bio.guoda.preston.process.EmittingStreamRDF;
import bio.guoda.preston.process.StatementsEmitterAdapter;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.KeyValueStoreLocalFileSystem;
import com.beust.jcommander.Parameters;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.io.output.NullPrintStream;
import org.apache.commons.rdf.api.Quad;

import java.io.InputStream;

@Parameters(separators = "= ", commandDescription = "Cites datasets in dataset archive")
public class CmdCite extends LoggingPersisting implements Runnable {

    @Override
    public void run() {
        BlobStoreReadOnly blobStoreAppendOnly
                = new BlobStoreAppendOnly(getKeyValueStore(new KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory(getHashType())), true, getHashType());
        run(resolvingBlobStore(blobStoreAppendOnly));

    }

    public void run(BlobStoreReadOnly blobStoreReadOnly) {
        StatementsListener listener = StatementLogFactory.createPrintingLogger(
                getLogMode(),
                NullOutputStream.NULL_OUTPUT_STREAM,
                () -> System.exit(0));

        CitationGenerator textMatcher = new CitationGenerator(
                this,
                blobStoreReadOnly,
                listener);

        StatementsEmitterAdapter emitter = new StatementsEmitterAdapter() {

            @Override
            public void emit(Quad statement) {
                textMatcher.on(statement);
            }
        };

        new EmittingStreamRDF(emitter, this)
                .parseAndEmit(getInputStream());

    }

}

