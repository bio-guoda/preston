package org.globalbioticinteractions.preston.store;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.globalbioticinteractions.preston.DateUtil;
import org.globalbioticinteractions.preston.Hasher;
import org.globalbioticinteractions.preston.model.RefNodeString;

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
        URI subj = statement.getLeft();
        URI predicate = statement.getMiddle();
        URI object = statement.getRight();

        if (Predicate.WAS_DERIVED_FROM.equals(predicate) && subj == null && object != null) {
            URI mostRecentDerivedSubject = findMostRecentDerivedSubject(object);
            if (mostRecentDerivedSubject != null && shouldResolveOnMissingOnly()) {
                put(Pair.of(Predicate.WAS_DERIVED_FROM, object), mostRecentDerivedSubject);
            } else {
                if (getDereferencer() != null) {
                    InputStream data = getDereferencer().dereference(object);
                    URI derivedSubject = blobStore.putBlob(data);
                    recordGenerationTime(derivedSubject);
                    if (null != mostRecentDerivedSubject && !mostRecentDerivedSubject.equals(derivedSubject)) {
                        put(Pair.of(Predicate.WAS_REVISION_OF, mostRecentDerivedSubject), derivedSubject);
                    } else if (null != derivedSubject) {
                        put(Pair.of(Predicate.WAS_DERIVED_FROM, object), derivedSubject);
                    }
                }
            }

        } else if (subj != null) {
            URI value = blobStore.putBlob(subj);
            put(Pair.of(predicate, object), value);
        }
    }

    private void recordGenerationTime(URI derivedSubject) throws IOException {
        String value = DateUtil.now() + "^^xsd:dateTime";
        blobStore.putBlob(IOUtils.toInputStream(value, StandardCharsets.UTF_8));
        put(Pair.of(derivedSubject, Predicate.GENERATED_AT_TIME), Hasher.calcSHA256(value));
    }

    public boolean shouldResolveOnMissingOnly() {
        return resolveOnMissingOnly;
    }

    public void setResolveOnMissingOnly(boolean resolveOnMissingOnly) {
        this.resolveOnMissingOnly = resolveOnMissingOnly;
    }

    private URI findMostRecentDerivedSubject(URI obj) throws IOException {
        URI existingId = findKey(Pair.of(Predicate.WAS_DERIVED_FROM, obj));
        return null == existingId ? null : findLastVersionId(existingId);
    }

    private URI findLastVersionId(URI existingId) throws IOException {
        URI lastVersionId = existingId;
        URI newerVersionId;
        while ((newerVersionId = findKey(Pair.of(Predicate.WAS_REVISION_OF, lastVersionId))) != null) {
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
