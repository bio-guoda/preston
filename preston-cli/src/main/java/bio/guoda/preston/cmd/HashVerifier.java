package bio.guoda.preston.cmd;

import bio.guoda.preston.HashGenerator;
import bio.guoda.preston.process.StatementsListenerAdapter;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.HashKeyUtil;
import bio.guoda.preston.store.KeyToPath;
import bio.guoda.preston.store.VersionUtil;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.util.Map;

public class HashVerifier extends StatementsListenerAdapter {
    private final Map<String, VerificationState> verifiedMap;
    private final BlobStoreReadOnly blobStore;
    private final HashGenerator<IRI> hashGenerator;
    private final boolean skipHashVerification;
    private final KeyToPath keyToPath;
    private final OutputStream outputStream;

    public HashVerifier(Map<String, VerificationState> verifiedMap,
                        BlobStoreReadOnly blobStore,
                        HashGenerator<IRI> hashGenerator,
                        boolean skipHashVerification,
                        OutputStream outputStream,
                        KeyToPath keyToPathLocal) {
        this.outputStream = outputStream;
        this.verifiedMap = verifiedMap;
        this.blobStore = blobStore;
        this.hashGenerator = hashGenerator;
        this.keyToPath = keyToPathLocal;
        this.skipHashVerification = skipHashVerification;
    }

    @Override
    public void on(Quad statement) {
        final IRI iri = VersionUtil.mostRecentVersionForStatement(statement);
        if (iri != null && !verifiedMap.containsKey(iri.getIRIString())) {
            VerificationState state = VerificationState.MISSING;
            if (HashKeyUtil.isValidHashKey(iri)) {
                verify(iri, state);
            } else {
                skip(iri);
            }
        }
    }

    private void skip(IRI iri) {
        handleVerificationEntry(iri,
                VerificationState.UNSUPPORTED_CONTENT_HASH,
                null,
                null);
    }

    void verify(IRI iri, VerificationState state) {
        IRI calculatedHashIRI = null;
        Long fileSize = null;
        try (InputStream is = blobStore.get(iri)) {
            if (is != null) {
                if (skipHashVerification) {
                    // try a shortcut via filesystem
                    URI uri = keyToPath.toPath(iri);
                    fileSize = new File(uri).length();
                    if (fileSize == 0) {
                        try (CountingOutputStream counting = new CountingOutputStream(NullOutputStream.NULL_OUTPUT_STREAM)) {
                            IOUtils.copy(is, counting);
                            fileSize = counting.getByteCount();
                        }
                    }
                    state = VerificationState.CONTENT_PRESENT_HASH_NOT_VERIFIED;

                } else {
                    try (CountingOutputStream counting = new CountingOutputStream(NullOutputStream.NULL_OUTPUT_STREAM)) {
                        calculatedHashIRI = hashGenerator.hash(is, counting);
                        state = calculatedHashIRI.equals(iri)
                                ? VerificationState.CONTENT_PRESENT_VALID_HASH
                                : VerificationState.CONTENT_PRESENT_INVALID_HASH;
                        fileSize = counting.getByteCount();
                    }
                }
            }
        } catch (IOException e) {
            //
        } finally {
            handleVerificationEntry(iri, state, calculatedHashIRI, fileSize);
        }
    }

    private void handleVerificationEntry(IRI iri, VerificationState state, IRI calculatedHashIRI, Long fileSize) {
        verifiedMap.put(iri.getIRIString(), state);

        String uriString = HashKeyUtil.isValidHashKey(iri)
                ? keyToPath.toPath(iri).toString()
                : iri.getIRIString();
        String msg = writeVerificationLogEntry(iri, state, calculatedHashIRI, fileSize, uriString);
        new PrintStream(outputStream)
                .print(msg);
    }

    private String writeVerificationLogEntry(IRI iri,
                                             VerificationState state,
                                             IRI calculatedHashIRI,
                                             Long fileSize,
                                             String uriString) {
        String stateString = "FAIL";
        if (CmdVerify.OK_STATES.contains(state)) {
            stateString = "OK";
        } else if (CmdVerify.SKIP_STATES.contains(state)) {
            stateString = "SKIP";
        }

        return iri.getIRIString() + "\t" +
                uriString + "\t" +
                stateString + "\t" +
                state + "\t" +
                (fileSize == null ? "" : fileSize) + "\t" +
                (calculatedHashIRI == null ? "" : calculatedHashIRI.getIRIString()) +
                "\n";
    }
}
