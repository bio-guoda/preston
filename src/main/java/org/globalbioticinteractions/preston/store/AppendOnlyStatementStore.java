package org.globalbioticinteractions.preston.store;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.globalbioticinteractions.preston.Hasher;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;


public class AppendOnlyStatementStore implements StatementStore<URI> {

    private BlobStore blobStore;

    private final Persistence persistence;

    private Dereferencer dereferencer;

    private boolean resolveOnMissingOnly = false;

    public AppendOnlyStatementStore(BlobStore blobStore, Persistence persistence, Dereferencer dereferencer) {
        this.blobStore = blobStore;
        this.persistence = persistence;
        this.dereferencer = dereferencer;
    }

    @Override
    public void put(Triple<URI, URI, URI> statement) throws IOException {
        URI object = statement.getRight();
        URI predicate = statement.getMiddle();
        URI subj = statement.getLeft();

        if (Predicate.HAS_CONTENT.equals(predicate) && object == null) {
            URI mostRecentVersionId = findMostRecentVersion(subj);
            if (mostRecentVersionId != null && shouldResolveOnMissingOnly()) {
                put(Pair.of(subj, Predicate.HAS_CONTENT_HASH), mostRecentVersionId);
            } else {
                if (getDereferencer() != null) {
                    InputStream data = getDereferencer().dereference(subj);
                    URI updatedId = blobStore.putBlob(data);
                    if (null != mostRecentVersionId && !mostRecentVersionId.equals(updatedId)) {
                        put(Pair.of(mostRecentVersionId, Predicate.HAD_REVISION), updatedId);
                        put(Pair.of(updatedId, Predicate.HAS_CONTENT_HASH), updatedId);
                    } else if (null != updatedId) {
                        put(Pair.of(subj, Predicate.HAS_CONTENT_HASH), updatedId);
                    }
                }
            }

        } else if (object != null) {
            URI value = blobStore.putBlob(object);
            put(Pair.of(subj, predicate), value);
        }
    }

    public boolean shouldResolveOnMissingOnly() {
        return resolveOnMissingOnly;
    }

    public void setResolveOnMissingOnly(boolean resolveOnMissingOnly) {
        this.resolveOnMissingOnly = resolveOnMissingOnly;
    }

    private URI findMostRecentVersion(URI subj) throws IOException {
        URI existingId = findKey(Pair.of(subj, Predicate.HAS_CONTENT_HASH));
        return null == existingId ? null : findLastVersionId(existingId);
    }

    private URI findLastVersionId(URI existingId) throws IOException {
        URI lastVersionId = existingId;
        URI newerVersionId;
        while ((newerVersionId = findKey(Pair.of(lastVersionId, Predicate.HAD_REVISION))) != null) {
            lastVersionId = newerVersionId;
        }
        return lastVersionId;
    }

    @Override
    public void put(Pair<URI, URI> partialStatement, URI value) throws IOException {
        // write-once, read-many
        URI key = calculateKeyFor(partialStatement);
        persistence.put(key.toString(), value.toString());
    }

    private URI calculateKeyFor(Pair<URI, URI> unhashedKeyPair) {
        URI left = Hasher.calcSHA256(unhashedKeyPair.getLeft().toString());
        URI right = Hasher.calcSHA256(unhashedKeyPair.getRight().toString());
        return Hasher.calcSHA256(left.toString() + right.toString());
    }

    @Override
    public URI findKey(Pair<URI, URI> partialStatement) throws IOException {
        InputStream inputStream = persistence.get(calculateKeyFor(partialStatement).toString());
        return inputStream == null
                ? null
                : URI.create(IOUtils.toString(inputStream, StandardCharsets.UTF_8));
    }


    public Dereferencer getDereferencer() {
        return dereferencer;
    }

    public void setDereferencer(Dereferencer dereferencer) {
        this.dereferencer = dereferencer;
    }
}
