package org.globalbioticinteractions.preston.store;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.globalbioticinteractions.preston.DateUtil;
import org.globalbioticinteractions.preston.Hasher;
import org.globalbioticinteractions.preston.cmd.CmdList;
import org.globalbioticinteractions.preston.model.RefNode;
import org.globalbioticinteractions.preston.model.RefNodeString;
import org.globalbioticinteractions.preston.model.RefStatement;
import org.globalbioticinteractions.preston.process.RefNodeFromKey;
import org.globalbioticinteractions.preston.process.RefNodeUtil;
import org.globalbioticinteractions.preston.process.RefStatementListener;
import org.globalbioticinteractions.preston.process.RefStatementProcessor;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import static org.globalbioticinteractions.preston.RefNodeConstants.GENERATED_AT_TIME;


public class AppendOnlyStatementStore extends RefStatementProcessor implements StatementStore<URI> {
    private static Log LOG = LogFactory.getLog(CmdList.class);

    private BlobStore blobStore;

    private final Persistence persistence;

    private Dereferencer dereferencer;

    private boolean resolveOnMissingOnly = false;

    public AppendOnlyStatementStore(BlobStore blobStore, Persistence persistence, Dereferencer dereferencer, RefStatementListener[] listener) {
        super(listener);
        this.blobStore = blobStore;
        this.persistence = persistence;
        this.dereferencer = dereferencer;
    }

    public AppendOnlyStatementStore(BlobStore blobStore, Persistence persistence, Dereferencer dereferencer) {
        this.blobStore = blobStore;
        this.persistence = persistence;
        this.dereferencer = dereferencer;
    }

    @Override
    public void on(RefStatement statement) {
        try {
            RefNode s = statement.getSubject();
            RefNode p = statement.getPredicate();
            RefNode o = statement.getObject();

            URI subject = getURI(s);
            URI predicate = getURI(p);
            URI object = getURI(o);

            put(Triple.of(subject, predicate, object));

        } catch (IOException e) {
            LOG.warn("failed to handle [" + statement.getLabel() + "]", e);
        }

    }

    private URI getURI(RefNode source) throws IOException {
        String s = source == null || source.getLabel() == null ? null : source.getLabel();
        return s == null ? null : URI.create(s);
    }


    @Override
    public void put(Triple<URI, URI, URI> statement) throws IOException {
        URI subj = statement.getLeft();
        URI predicate = statement.getMiddle();
        URI object = statement.getRight();

        if (Predicate.WAS_DERIVED_FROM.equals(predicate) && subj == null && object != null) {
            URI mostRecent = findMostRecent(object);
            if (mostRecent == null || !shouldResolveOnMissingOnly()) {
                if (getDereferencer() != null) {
                    InputStream data = getDereferencer().dereference(object);
                    URI derivedSubject = blobStore.putBlob(data);
                    if (null != mostRecent && !mostRecent.equals(derivedSubject)) {
                        recordGenerationTime(derivedSubject);
                        put(Pair.of(Predicate.WAS_REVISION_OF, mostRecent), derivedSubject);
                        Triple<URI, URI, URI> of = Triple.of(derivedSubject, Predicate.WAS_REVISION_OF, mostRecent);
                        emit(new RefStatement(new RefNodeFromKey(blobStore, of.getLeft()),
                                new RefNodeString(of.getMiddle().toString()),
                                new RefNodeFromKey(blobStore, of.getRight())));

                    } else if (null == mostRecent) {
                        recordGenerationTime(derivedSubject);
                        put(Pair.of(Predicate.WAS_DERIVED_FROM, object), derivedSubject);
                        Triple<URI, URI, URI> of = Triple.of(derivedSubject, Predicate.WAS_DERIVED_FROM, object);
                        emit(new RefStatement(new RefNodeFromKey(blobStore, of.getLeft()),
                                new RefNodeString(of.getMiddle().toString()),
                                new RefNodeString(of.getRight().toString())));
                    } else {

                    }
                }
            }
        } else if (subj != null && predicate != null && object != null) {
            emit(new RefStatement(new RefNodeString(subj.toString()),
                    new RefNodeString(predicate.toString()),
                    new RefNodeString(object.toString())));

        }
    }

    private void recordGenerationTime(URI derivedSubject) throws IOException {
        String value = RefNodeUtil.toDateTime(DateUtil.now()).getLabel();
        blobStore.putBlob(IOUtils.toInputStream(value, StandardCharsets.UTF_8));
        URI value1 = Hasher.calcSHA256(value);
        put(Pair.of(derivedSubject, Predicate.GENERATED_AT_TIME), value1);
        emit(new RefStatement(new RefNodeFromKey(blobStore, derivedSubject),
                GENERATED_AT_TIME,
                new RefNodeString(value)));

    }

    public boolean shouldResolveOnMissingOnly() {
        return resolveOnMissingOnly;
    }

    public void setResolveOnMissingOnly(boolean resolveOnMissingOnly) {
        this.resolveOnMissingOnly = resolveOnMissingOnly;
    }

    private URI findMostRecent(URI obj) throws IOException {
        URI existingId = findKey(Pair.of(Predicate.WAS_DERIVED_FROM, obj));

        if (existingId != null) {
            emitExistingVersion(existingId, Predicate.WAS_DERIVED_FROM, new RefNodeString(obj.toString()));
            existingId = findLastVersionId(existingId);
        }
        return existingId;
    }

    private URI findLastVersionId(URI existingId) throws IOException {
        URI lastVersionId = existingId;
        URI newerVersionId;
        while ((newerVersionId = findKey(Pair.of(Predicate.WAS_REVISION_OF, lastVersionId))) != null) {
            emitExistingVersion(newerVersionId, Predicate.WAS_REVISION_OF, new RefNodeFromKey(blobStore, lastVersionId));
            lastVersionId = newerVersionId;
        }
        return lastVersionId;
    }

    private void emitExistingVersion(URI subj, URI predicate, RefNode obj) throws IOException {
        URI timeKey = findKey(Pair.of(subj, Predicate.GENERATED_AT_TIME));
        if (timeKey != null) {
            InputStream input = blobStore.get(timeKey);
            if (input != null) {
                emit(new RefStatement(new RefNodeFromKey(blobStore, subj),
                        GENERATED_AT_TIME,
                        new RefNodeString(IOUtils.toString(input, StandardCharsets.UTF_8))));
            }
        }
        emit(new RefStatement(new RefNodeFromKey(blobStore, subj),
                new RefNodeString(predicate.toString()),
                obj));
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
