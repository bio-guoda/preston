package bio.guoda.preston.cmd;

import bio.guoda.preston.StatementLogFactory;
import bio.guoda.preston.process.BlobStoreReadOnly;
import bio.guoda.preston.process.BloomFilterCreate;
import bio.guoda.preston.process.EmittingStreamRDF;
import bio.guoda.preston.process.StatementsEmitterAdapter;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.process.TextMatcher;
import bio.guoda.preston.store.BlobStore;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.KeyValueStoreLocalFileSystem;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.commons.rdf.api.Quad;

import java.io.IOException;
import java.io.InputStream;

@Parameters(separators = "= ", commandDescription = "creates bloom filters for matched content")
public class CmdBloomFilterCreate extends LoggingPersisting implements Runnable {

    private InputStream inputStream = System.in;


    @Override
    public void run() {
        BlobStoreAppendOnly blobStoreAppendOnly = new BlobStoreAppendOnly(getKeyValueStore(new KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory()));
        run(blobStoreAppendOnly);

    }

    public void run(BlobStore blobStore) {
        StatementsListener listener = StatementLogFactory.createPrintingLogger(
                getLogMode(),
                System.out, () -> System.exit(0));

        try (BloomFilterCreate bloomFilterCreate = new BloomFilterCreate(blobStore, listener)) {

            StatementsEmitterAdapter emitter = new StatementsEmitterAdapter() {

                @Override
                public void emit(Quad statement) {
                    bloomFilterCreate.on(statement);
                }
            };

            new EmittingStreamRDF(emitter, this)
                    .parseAndEmit(inputStream);
        } catch (IOException e) {
            throw new RuntimeException("failed to generate bloom filter", e);
        }

    }

    public void setInputStream(InputStream inputStream) {
        this.inputStream = inputStream;
    }

}

