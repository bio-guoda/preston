package bio.guoda.preston.cmd;

import bio.guoda.preston.HashGenerator;
import bio.guoda.preston.process.StatementsListenerAdapter;
import bio.guoda.preston.store.Dereferencer;
import bio.guoda.preston.store.HashKeyUtil;
import bio.guoda.preston.store.KeyToPath;
import bio.guoda.preston.store.VersionUtil;
import bio.guoda.preston.stream.ContentHashDereferencer;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDFTerm;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.util.Map;

import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;

public class HashVerifier extends StatementsListenerAdapter {
    private final Map<String, VerificationState> verifiedMap;
    private final Dereferencer<InputStream> blobStore;
    private final HashGenerator<IRI> hashGenerator;
    private final boolean skipHashVerification;
    private final KeyToPath keyToPath;
    private final OutputStream outputStream;

    public HashVerifier(Map<String, VerificationState> verifiedMap,
                        Dereferencer<InputStream> blobStore,
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
        final IRI iri = VersionUtil.mostRecentVersion(statement);
        if (iri != null && !verifiedMap.containsKey(iri.getIRIString())) {
            if (HashKeyUtil.isValidHashKey(iri)) {
                VerificationEntry entry = null;
                try {
                    if (HashVerifier.containsHashBasedContentRelationClaim(statement)) {
                        entry = verifyHashBasedRelation(statement);
                    } else {
                        entry = verifyLocationBasedRelation(iri);
                    }
                } catch (IOException e) {
                    //
                } finally {
                    if (entry != null) {
                        writeEntry(entry);
                    }
                }

            } else {
                writeEntry(
                        new VerificationEntry(iri,
                                VerificationState.UNSUPPORTED_CONTENT_HASH,
                                null,
                                null
                        )
                );
            }
        }
    }

    private VerificationEntry verifyHashBasedRelation(Quad statement) throws IOException {
        IRI subject = (IRI) statement.getSubject();
        VerificationEntry verificationEntry = verifyAuthenticity(
                (IRI) statement.getObject(),
                new ContentHashDereferencer(blobStore).get(subject),
                hashGenerator,
                VerificationState.MISSING
        );
        if (verificationEntry != null) {
            verificationEntry = new VerificationEntry(
                    subject,
                    verificationEntry.getState(),
                    verificationEntry.getCalculatedHashIRI(),
                    verificationEntry.getFileSize()
            );
        }
        return verificationEntry;
    }

    private VerificationEntry verifyLocationBasedRelation(IRI expectedHash) throws IOException {
        VerificationState state = VerificationState.MISSING;
        VerificationEntry entry = new VerificationEntry(
                expectedHash,
                state,
                null,
                null
        );
        try (InputStream is = blobStore.get(expectedHash)) {
            if (is != null) {
                if (skipHashVerification) {
                    entry = verifyExistence(expectedHash, state, is);

                } else {
                    entry = verifyAuthenticity(expectedHash, is, hashGenerator, state);
                }
            }
            return entry;
        }
    }

    private VerificationEntry verifyExistence(IRI iri, VerificationState state, InputStream is) throws IOException {
        VerificationEntry entry = new VerificationEntry(iri, state, null, null);
        // try a shortcut via filesystem
        URI uri = keyToPath.toPath(iri);
        Long fileSize = new File(uri).length();
        if (fileSize == 0) {
            try (CountingOutputStream counting = new CountingOutputStream(NullOutputStream.INSTANCE)) {
                IOUtils.copy(is, counting);
                entry.setFileSize(counting.getByteCount());
            }
        } else {
            entry.setFileSize(fileSize);
        }
        entry.setState(VerificationState.CONTENT_PRESENT_HASH_NOT_VERIFIED);
        return entry;
    }

    private static VerificationEntry verifyAuthenticity(IRI expectedHash, InputStream is, HashGenerator<IRI> hashGenerator, VerificationState state) throws IOException {
        VerificationEntry entry = new VerificationEntry(expectedHash, state, null, null);
        try (CountingOutputStream counting = new CountingOutputStream(NullOutputStream.INSTANCE)) {
            IRI hash = hashGenerator.hash(is, counting);
            entry.setCalculatedHashIRI(hash);
            entry.setState(entry.getCalculatedHashIRI() != null && entry.getCalculatedHashIRI().equals(expectedHash)
                    ? VerificationState.CONTENT_PRESENT_VALID_HASH
                    : VerificationState.CONTENT_PRESENT_INVALID_HASH);
            entry.setFileSize(counting.getByteCount());
        }
        return entry;
    }

    private void writeEntry(VerificationEntry verificationEntry) {
        verifiedMap.put(verificationEntry.getIri().getIRIString(), verificationEntry.getState());

        String uriString = HashKeyUtil.isValidHashKey(verificationEntry.getIri())
                ? keyToPath.toPath(verificationEntry.getIri()).toString()
                : verificationEntry.getIri().getIRIString();
        String msg = writeVerificationLogEntry(verificationEntry.getIri(), verificationEntry.getState(), verificationEntry.getCalculatedHashIRI(), verificationEntry.getFileSize(), uriString);
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

    public static boolean containsHashBasedContentRelationClaim(Quad statement) {
        boolean containsHashBasedContentRelationClaim = false;
        if (statement.getPredicate().equals(HAS_VERSION)) {
            BlankNodeOrIRI subject = statement.getSubject();
            RDFTerm object = statement.getObject();
            if (subject instanceof IRI && object instanceof IRI) {
                IRI derivedFromHash = (IRI) subject;
                IRI claimedDerivedVersion = (IRI) object;
                if (HashKeyUtil.isValidHashKey(derivedFromHash) && HashKeyUtil.isValidHashKey(claimedDerivedVersion)) {
                    containsHashBasedContentRelationClaim = true;
                }
            }
        }
        return containsHashBasedContentRelationClaim;
    }

    public static boolean verifyContentRelationClaim(Dereferencer<InputStream> blobStore,
                                                     IRI derivedFromHash,
                                                     IRI claimedDerivedVersion,
                                                     HashGenerator<IRI> hashGenerator) throws IOException {
        boolean verified = false;
        IRI calculatedVersion = hashGenerator.hash(blobStore.get(derivedFromHash));
        if (StringUtils.equals(calculatedVersion.getIRIString(), claimedDerivedVersion.getIRIString())) {
            verified = true;
        }
        return verified;
    }


}
