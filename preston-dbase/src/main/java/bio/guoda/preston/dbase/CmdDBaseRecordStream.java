package bio.guoda.preston.dbase;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.cmd.LoggingPersisting;
import bio.guoda.preston.process.EmittingStreamRDF;
import bio.guoda.preston.process.StatementsEmitterAdapter;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.ValidatingKeyValueStreamContentAddressedFactory;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import picocli.CommandLine;

import java.io.IOException;

@CommandLine.Command(
        name = "dbase-json-stream",
        aliases = {"dbase2json", "dbase-stream"},
        description = "Extract records from DBase files (*.DBF) in line-json"
)
public class CmdDBaseRecordStream extends LoggingPersisting implements Runnable {

    @Override
    public void run() {
        BlobStoreReadOnly blobStoreAppendOnly
                = new BlobStoreAppendOnly(getKeyValueStore(new ValidatingKeyValueStreamContentAddressedFactory(getHashType())), true, getHashType());
        run(resolvingBlobStore(blobStoreAppendOnly));

    }

    public void run(BlobStoreReadOnly blobStoreReadOnly) {
        StatementsEmitterAdapter emitter = new StatementsEmitterAdapter() {

            @Override
            public void emit(Quad statement) {
                if (RefNodeFactory.hasVersionAvailable(statement)) {
                    BlankNodeOrIRI version = RefNodeFactory.getVersion(statement);
                    try {
                        DBaseHandler.asJsonStream(
                                getOutputStream(),
                                (IRI) version,
                                blobStoreReadOnly);
                    } catch (IOException e) {
                        // ignore
                    }
                }

            }
        };

        new EmittingStreamRDF(emitter, this)
                .parseAndEmit(getInputStream());

    }

}

