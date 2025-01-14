package bio.guoda.preston.cmd;

import bio.guoda.preston.HashGenerator;
import bio.guoda.preston.HashGeneratorFactory;
import bio.guoda.preston.process.EmittingStreamFactory;
import bio.guoda.preston.process.EmittingStreamOfAnyVersions;
import bio.guoda.preston.process.ParsingEmitter;
import bio.guoda.preston.process.ProcessorState;
import bio.guoda.preston.process.StatementEmitter;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.BlobStore;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.KeyToPathFactoryDepth;
import bio.guoda.preston.store.ValidatingKeyValueStreamContentAddressedFactory;
import org.apache.commons.rdf.api.IRI;
import picocli.CommandLine;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static bio.guoda.preston.cmd.ReplayUtil.attemptReplay;

@CommandLine.Command(
        name = "test",
        aliases = {"verify", "check", "validate", "touch"},
        description = "Verifies completeness and, optionally, the integrity, of a biodiversity dataset graph by touching (or visiting) all associated content."
)
public class CmdVerify extends Persisting implements Runnable {

    public static final List<VerificationState> OK_STATES = Arrays.asList(
            VerificationState.CONTENT_PRESENT_VALID_HASH,
            VerificationState.CONTENT_PRESENT_HASH_NOT_VERIFIED);

    public static final List<VerificationState> SKIP_STATES = Arrays.asList(
            VerificationState.UNSUPPORTED_CONTENT_HASH);

    public static final List<VerificationState> FAIL_STATES = Arrays.asList(
            VerificationState.CONTENT_PRESENT_INVALID_HASH,
            VerificationState.MISSING);


    public static final String DO_NOT_VERIFY_HASH_JUST_CHECK_AVAILABILITY = "Do not verify hash, just check availability";

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
                = new BlobStoreAppendOnly(
                getKeyValueStore(new ValidatingKeyValueStreamContentAddressedFactory()),
                true,
                getHashType()
        );

        Map<String, VerificationState> verifiedMap = new TreeMap<>();

        StatementsListener statementListener = new HashVerifier(
                verifiedMap,
                blobStore,
                hashGenerator,
                skipHashVerification,
                getOutputStream(),
                new KeyToPathFactoryDepth(new File(getDataDir()).toURI(), getDepth()).getKeyToPath()
        );

        CmdContext ctx = new CmdContext(this, getProvenanceAnchor(), statementListener);

        attemptReplay(
                blobStore,
                ctx,
                getProvenanceTracer(),
                new EmittingStreamFactory() {
                    @Override
                    public ParsingEmitter createEmitter(StatementEmitter emitter, ProcessorState context) {
                        return new EmittingStreamOfAnyVersions(emitter, context);
                    }
                });
    }


}
