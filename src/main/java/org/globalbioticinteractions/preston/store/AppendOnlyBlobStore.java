package org.globalbioticinteractions.preston.store;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.globalbioticinteractions.preston.Hasher;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;


public class AppendOnlyBlobStore implements BlobStore, RelationStore {

    private Dereferencer dereferencer;

    private final Persistence persistence;

    public AppendOnlyBlobStore(Dereferencer dereferencer, Persistence persistence) {
        this.dereferencer = dereferencer;
        this.persistence = persistence;
    }

    private Dereferencer getDereferencer() {
        return this.dereferencer;
    }
    // write-once, read-many

    @Override
    public String putBlob(InputStream is) throws IOException {
        return persistence.put(Hasher::calcSHA256, is);
    }

    @Override
    public String putBlob(URI entity) throws IOException {
        return putBlob(IOUtils.toInputStream(entity.toString(), StandardCharsets.UTF_8));
    }

    @Override
    public InputStream get(String key) throws IOException {
        return key == null ? null : persistence.get(key);
    }

    @Override
    public void put(Triple<URI, URI, URI> statement) throws IOException {
        URI object = statement.getRight();
        URI predicate = statement.getMiddle();
        URI subj = statement.getLeft();

        if (Predicate.HAS_CONTENT == predicate && object == null) {
            String mostRecentVersionId = findMostRecentVersion(subj);

            String updatedId = null;
            if (getDereferencer() != null) {
                InputStream data = getDereferencer().dereference(subj);
                updatedId = putBlob(data);
            }

            if (StringUtils.isNotBlank(mostRecentVersionId) && !StringUtils.equals(mostRecentVersionId, updatedId)) {
                put(Pair.of(URI.create("preston:" + mostRecentVersionId), Predicate.SUCCEEDED_BY), updatedId);
                put(Pair.of(URI.create("preston:" + updatedId), Predicate.HAS_CONTENT_HASH), updatedId);
            } else if (StringUtils.isNotBlank(updatedId)) {
                put(Pair.of(subj, Predicate.HAS_CONTENT_HASH), updatedId);
            }
        } else {
            put(Pair.of(subj, predicate), putBlob(object));
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
    public void put(Pair<URI, URI> unhashedKeyPair, String value) throws IOException {
        // write-once, read-many
        String key = calculateKeyFor(unhashedKeyPair);

        persistence.put(key, value);
    }

    private String calculateKeyFor(Pair<URI, URI> unhashedKeyPair) {
        String left = Hasher.calcSHA256(unhashedKeyPair.getLeft().toString());
        String right = Hasher.calcSHA256(unhashedKeyPair.getRight().toString());
        return Hasher.calcSHA256(left + right);
    }

    @Override
    public String findKey(Pair<URI, URI> unhashedKeyPair) throws IOException {
        InputStream inputStream = persistence.get(calculateKeyFor(unhashedKeyPair));
        return inputStream == null ? null : IOUtils.toString(inputStream, StandardCharsets.UTF_8);
    }


    void setDereferencer(Dereferencer dereferencer) {
        this.dereferencer = dereferencer;
    }
}
