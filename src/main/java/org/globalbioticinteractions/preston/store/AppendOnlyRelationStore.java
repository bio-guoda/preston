package org.globalbioticinteractions.preston.store;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.globalbioticinteractions.preston.Hasher;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;


public class AppendOnlyRelationStore implements RelationStore<URI> {

    private BlobStore blobStore;

    private final Persistence persistence;

    private Dereferencer dereferencer;

    public AppendOnlyRelationStore(BlobStore blobStore, Persistence persistence, Dereferencer dereferencer) {
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
            String mostRecentVersionId = findMostRecentVersion(subj);

            String updatedId = null;
            if (getDereferencer() != null) {
                InputStream data = getDereferencer().dereference(subj);
                updatedId = blobStore.putBlob(data);
            }

            if (StringUtils.isNotBlank(mostRecentVersionId) && !StringUtils.equals(mostRecentVersionId, updatedId)) {
                put(Pair.of(URI.create("preston:" + mostRecentVersionId), Predicate.SUCCEEDED_BY), updatedId);
                put(Pair.of(URI.create("preston:" + updatedId), Predicate.HAS_CONTENT_HASH), updatedId);
            } else if (StringUtils.isNotBlank(updatedId)) {
                put(Pair.of(subj, Predicate.HAS_CONTENT_HASH), updatedId);
            }
        } else if (object != null) {
            String value = blobStore.putBlob(object);
            put(Pair.of(subj, predicate), value);
        }
    }
    
    private String findMostRecentVersion(URI subj) throws IOException {
        String existingId = findKey(Pair.of(subj, Predicate.HAS_CONTENT_HASH));
        return StringUtils.isBlank(existingId) ? null : findLastVersionId(existingId);
    }

    private String findLastVersionId(String existingId) throws IOException {
        String lastVersionId = existingId;
        String newerVersionId;
        while ((newerVersionId = findKey(Pair.of(URI.create("preston:" + lastVersionId), Predicate.SUCCEEDED_BY))) != null) {
            lastVersionId = newerVersionId;
        }
        return lastVersionId;
    }

    @Override
    public void put(Pair<URI, URI> partialStatement, String value) throws IOException {
        // write-once, read-many
        String key = calculateKeyFor(partialStatement);
        persistence.put(key, value);
    }

    private String calculateKeyFor(Pair<URI, URI> unhashedKeyPair) {
        String left = Hasher.calcSHA256(unhashedKeyPair.getLeft().toString());
        String right = Hasher.calcSHA256(unhashedKeyPair.getRight().toString());
        return Hasher.calcSHA256(left + right);
    }

    @Override
    public String findKey(Pair<URI, URI> partialStatement) throws IOException {
        InputStream inputStream = persistence.get(calculateKeyFor(partialStatement));
        return inputStream == null ? null : IOUtils.toString(inputStream, StandardCharsets.UTF_8);
    }


    public Dereferencer getDereferencer() {
        return dereferencer;
    }

    public void setDereferencer(Dereferencer dereferencer) {
        this.dereferencer = dereferencer;
    }
}
