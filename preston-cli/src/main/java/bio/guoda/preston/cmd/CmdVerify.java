package bio.guoda.preston.cmd;

import bio.guoda.preston.HashGenerator;
import bio.guoda.preston.HashGeneratorFactory;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.process.StatementsListenerAdapter;
import bio.guoda.preston.store.BlobStore;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.KeyValueStoreLocalFileSystem;
import bio.guoda.preston.store.VersionUtil;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static bio.guoda.preston.cmd.ReplayUtil.attemptReplay;

@Parameters(separators = "= ", commandDescription = CmdVerify.VERIFIES_COMPLETENESS_AND_INTEGRITY_OF_THE_LOCAL_BIODIVERSITY_DATASET_GRAPH)
@CommandLine.Command(
        name = "test",
        aliases = {"verify", "check", "validate"},
        description = CmdVerify.VERIFIES_COMPLETENESS_AND_INTEGRITY_OF_THE_LOCAL_BIODIVERSITY_DATASET_GRAPH
)
public class CmdVerify extends PersistingLocal implements Runnable {

    public static final List<State> OK_STATES = Arrays.asList(
            State.CONTENT_PRESENT_VALID_HASH,
            State.CONTENT_PRESENT_HASH_NOT_VERIFIED);
    public static final String VERIFIES_COMPLETENESS_AND_INTEGRITY_OF_THE_LOCAL_BIODIVERSITY_DATASET_GRAPH = "Verifies completeness and integrity of the local biodiversity dataset graph";
    public static final String DO_NOT_VERIFY_HASH_JUST_CHECK_AVAILABILITY = "Do not verify hash, just check availability";

    enum State {
        MISSING,
        CONTENT_PRESENT_INVALID_HASH,
        CONTENT_PRESENT_VALID_HASH,
        CONTENT_PRESENT_HASH_NOT_VERIFIED
    }

    @Parameter(names = {"--skip-hash-verification"}, description = DO_NOT_VERIFY_HASH_JUST_CHECK_AVAILABILITY)
    @CommandLine.Option(
            names = "--skip-hash-verification",
            description = DO_NOT_VERIFY_HASH_JUST_CHECK_AVAILABILITY
    )
    private Boolean skipHashVerification = false;

    @Override
    public void run() {

        final HashGenerator<IRI> hashGenerator
                = new HashGeneratorFactory().create(getHashType());

        final BlobStore blobStore
                = new BlobStoreAppendOnly(getKeyValueStore(new KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory(getHashType())), true, getHashType());

        Map<String, State> verifiedMap = new TreeMap<>();

        StatementsListener statementListener = new StatementsListenerAdapter() {
            @Override
            public void on(Quad statement) {
                final IRI iri = VersionUtil.mostRecentVersionForStatement(statement);
                if (iri != null && !verifiedMap.containsKey(iri.getIRIString())) {
                    State state = State.MISSING;
                    IRI calculatedHashIRI = null;
                    long fileSize = 0;
                    try (InputStream is = blobStore.get(iri)) {
                        if (is != null) {
                            if (skipHashVerification) {
                                // try a shortcut via filesystem
                                URI uri = getKeyToPathLocal().toPath(iri);
                                fileSize = new File(uri).length();
                                if (fileSize == 0) {
                                    try (CountingOutputStream counting = new CountingOutputStream(NullOutputStream.NULL_OUTPUT_STREAM)) {
                                        IOUtils.copy(is, counting);
                                        fileSize = counting.getByteCount();
                                    }
                                }
                                state = State.CONTENT_PRESENT_HASH_NOT_VERIFIED;
                            } else {
                                try (CountingOutputStream counting = new CountingOutputStream(NullOutputStream.NULL_OUTPUT_STREAM)) {
                                    calculatedHashIRI = hashGenerator.hash(is, counting);
                                    state = calculatedHashIRI.equals(iri) ? State.CONTENT_PRESENT_VALID_HASH : State.CONTENT_PRESENT_INVALID_HASH;
                                    fileSize = counting.getByteCount();
                                }
                            }
                        }
                    } catch (IOException e) {
                        //
                    } finally {
                        verifiedMap.put(iri.getIRIString(), state);
                        new PrintStream(getOutputStream()).print(iri.getIRIString() + "\t" +
                                getKeyToPathLocal().toPath(iri) + "\t" +
                                (OK_STATES.contains(state) ? "OK" : "FAIL") + "\t" +
                                state + "\t" +
                                fileSize + "\t" +
                                (calculatedHashIRI == null ? "" : calculatedHashIRI.getIRIString()) +
                                "\n");
                    }
                }
            }
        };
        CmdContext ctx = new CmdContext(this, statementListener);

        attemptReplay(blobStore, ctx, getTracerOfDescendants());
    }


}
