package bio.guoda.preston.paradox;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.cmd.LoggingPersisting;
import bio.guoda.preston.process.EmittingStreamOfAnyQuad;
import bio.guoda.preston.process.StatementsEmitterAdapter;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.ValidatingKeyValueStreamContentAddressedFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import picocli.CommandLine;

import java.io.IOException;
import java.net.URI;

@CommandLine.Command(
        name = "paradox-stream",
        aliases = {"paradox2json"},
        hidden = true,
        description = "Extract records from Paradox Database files (*.DB) in line-json"
)
public class CmdParadoxRecordStream extends LoggingPersisting implements Runnable {

    @Override
    public void run() {
        BlobStoreReadOnly blobStoreAppendOnly
                = new BlobStoreAppendOnly(getKeyValueStore(new ValidatingKeyValueStreamContentAddressedFactory()), true, getHashType());
        run(resolvingBlobStore(blobStoreAppendOnly));

    }

    public void run(BlobStoreReadOnly blobStoreReadOnly) {
        StatementsEmitterAdapter emitter = new StatementsEmitterAdapter() {

            @Override
            public void emit(Quad statement) {
                if (RefNodeFactory.hasVersionAvailable(statement)) {
                    BlankNodeOrIRI version = RefNodeFactory.getVersion(statement);
                    IRI versionSource = RefNodeFactory.getVersionSource(statement);
                    URI uri = URI.create(versionSource.getIRIString());
                    String[] parts = StringUtils.split(uri.getPath(), "/");
                    final String tableNameCandidate = (parts != null && parts.length > 0)
                            ? parts[parts.length - 1]
                            : "table";

                    try {
                        ParadoxHandler.asJsonStream(
                                getOutputStream(),
                                (IRI) version,
                                tableNameCandidate,
                                blobStoreReadOnly,
                                CmdParadoxRecordStream.this);
                    } catch (IOException e) {
                        // ignore
                    }
                }
            }

        };

        new EmittingStreamOfAnyQuad(emitter, this)
                .parseAndEmit(getInputStream());

    }

}

