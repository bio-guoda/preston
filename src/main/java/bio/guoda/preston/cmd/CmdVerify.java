package bio.guoda.preston.cmd;

import bio.guoda.preston.HashGenerator;
import bio.guoda.preston.HashGeneratorFactory;
import bio.guoda.preston.HashType;
import bio.guoda.preston.Hasher;
import bio.guoda.preston.process.StatementListener;
import bio.guoda.preston.store.BlobStore;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.KeyValueStoreLocalFileSystem;
import bio.guoda.preston.store.StatementStore;
import bio.guoda.preston.store.StatementStoreImpl;
import bio.guoda.preston.store.VersionUtil;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static bio.guoda.preston.cmd.ReplayUtil.attemptReplay;

@Parameters(separators = "= ", commandDescription = "verifies completeness and integrity of the local biodiversity dataset graph")
public class CmdVerify extends PersistingLocal implements Runnable {

    public static final List<State> OK_STATES = Arrays.asList(
            State.CONTENT_PRESENT_VALID_HASH,
            State.CONTENT_PRESENT_HASH_NOT_VERIFIED);

    enum State {
        MISSING,
        CONTENT_PRESENT_INVALID_HASH,
        CONTENT_PRESENT_VALID_HASH,
        CONTENT_PRESENT_HASH_NOT_VERIFIED
    }

    ;

    @Parameter(names = {"--skip-hash-verification"}, description = "do not verify hash, just check availability")
    private Boolean skipHashVerification = false;


    @Parameter(names = {"--hash-algorithm"}, description = "hash algorithm used for verification")
    private HashType hashType = HashType.SHA256;


    @Override
    public void run() {

        final HashGenerator<IRI> hashGenerator = new HashGeneratorFactory().create(hashType);

        final BlobStore blobStore = new BlobStoreAppendOnly(getKeyValueStore(new KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory()));
        final StatementStore statementPersistence = new StatementStoreImpl(getKeyValueStore(new KeyValueStoreLocalFileSystem.KeyValueStreamFactorySHA256Values()));

        Map<String, State> verifiedMap = new TreeMap<>();

        StatementListener statementListener = new StatementListener() {
            @Override
            public void on(Quad statement) {
                final IRI iri = VersionUtil.mostRecentVersionForStatement(statement);
                if (iri != null && !verifiedMap.containsKey(iri.getIRIString())) {
                    State state = State.MISSING;
                    long fileSize = 0;
                    try (InputStream is = blobStore.get(iri)) {
                        IRI calculatedHashIRI = null;
                        if (is != null) {
                            if (skipHashVerification) {
                                // try a shortcut via filesystem
                                URI uri = getKeyToPathLocal().toPath(iri);
                                fileSize = new File(uri).length();
                                if (fileSize == 0) {
                                    try (CountingOutputStream counting = new CountingOutputStream(new NullOutputStream())) {
                                        IOUtils.copy(is, counting);
                                        fileSize = counting.getByteCount();
                                    }
                                }
                                state = State.CONTENT_PRESENT_HASH_NOT_VERIFIED;
                            } else {
                                try (CountingOutputStream counting = new CountingOutputStream(new NullOutputStream())) {
                                    calculatedHashIRI = hashGenerator.hash(is, counting);
                                    state = calculatedHashIRI.equals(iri) ? State.CONTENT_PRESENT_VALID_HASH : State.CONTENT_PRESENT_INVALID_HASH;
                                    fileSize = counting.getByteCount();
                                }
                            }
                        }
                        System.out.print(iri.getIRIString() + "\t" +
                                getKeyToPathLocal().toPath(iri) + "\t" +
                                (OK_STATES.contains(state) ? "OK" : "FAIL") + "\t" +
                                state + "\t" +
                                fileSize + "\t" +
                                (calculatedHashIRI == null ? "" : calculatedHashIRI.getIRIString()) +
                                "\n");

                    } catch (IOException e) {
                        //
                    } finally {
                        verifiedMap.put(iri.getIRIString(), state);
                    }
                }
            }
        };
        CmdContext ctx = new CmdContext(this, statementListener);

        attemptReplay(blobStore, statementPersistence, ctx);
    }

    public void setHashType(HashType hashType) {
        this.hashType = hashType;
    }

}
