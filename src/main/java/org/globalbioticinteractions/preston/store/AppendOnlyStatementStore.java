package org.globalbioticinteractions.preston.store;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.rdf.api.BlankNode;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDFTerm;
import org.apache.commons.rdf.api.Triple;
import org.globalbioticinteractions.preston.DateUtil;
import org.globalbioticinteractions.preston.Hasher;
import org.globalbioticinteractions.preston.RDFUtil;
import org.globalbioticinteractions.preston.cmd.CmdList;
import org.globalbioticinteractions.preston.model.RefNodeFactory;
import org.globalbioticinteractions.preston.process.RefStatementListener;
import org.globalbioticinteractions.preston.process.RefStatementProcessor;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import static org.globalbioticinteractions.preston.RefNodeConstants.GENERATED_AT_TIME;


public class AppendOnlyStatementStore extends RefStatementProcessor implements StatementStore {
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
    public void on(Triple statement) {
        try {
            put(statement);
        } catch (Throwable e) {
            LOG.warn("failed to handle [" + statement.toString() + "]", e);
        }

    }

    @Override
    public void put(Triple statement) throws IOException {
        BlankNodeOrIRI subj = statement.getSubject();
        IRI predicate = statement.getPredicate();
        RDFTerm object = statement.getObject();

        if (Predicate.WAS_DERIVED_FROM.equals(predicate)
                && subj instanceof BlankNode
                && object != null
                && object instanceof IRI) {

            IRI mostRecent = findMostRecent((IRI) object);
            if (mostRecent == null || !shouldResolveOnMissingOnly()) {
                if (getDereferencer() != null) {
                    try {
                        attemptUpdate((IRI) object, mostRecent);
                    } catch (IOException e) {
                        LOG.warn("failed to update [" + object.toString() + "]", e);
                    }
                }
            }
        } else if (subj != null && predicate != null && object != null) {
            emit(RefNodeFactory.toStatement(
                    subj,
                    predicate,
                    object));

        }
    }

    private void attemptUpdate(IRI object, IRI mostRecent) throws IOException {
        InputStream data = getDereferencer().dereference(object);
        IRI derivedSubject = blobStore.putBlob(data);
        if (null != mostRecent && !mostRecent.equals(derivedSubject)) {
            recordGenerationTime(derivedSubject);
            put(Pair.of(Predicate.WAS_REVISION_OF, mostRecent), derivedSubject);
            Triple of = RefNodeFactory.toStatement(derivedSubject, Predicate.WAS_REVISION_OF, mostRecent);
            emit(of);

        } else if (null == mostRecent) {
            recordGenerationTime(derivedSubject);
            put(Pair.of(Predicate.WAS_DERIVED_FROM, object), derivedSubject);
            emit(RefNodeFactory.toStatement(derivedSubject, Predicate.WAS_DERIVED_FROM, object));
        }
    }

    private void recordGenerationTime(IRI derivedSubject) throws IOException {
        String value = RefNodeFactory.toDateTime(DateUtil.now()).getLexicalForm();
        blobStore.putBlob(IOUtils.toInputStream(value, StandardCharsets.UTF_8));
        IRI value1 = Hasher.calcSHA256(value);

        Pair<RDFTerm, RDFTerm> of = Pair.of(derivedSubject, Predicate.GENERATED_AT_TIME);
        put(of, value1);
        emit(RefNodeFactory.toStatement(derivedSubject,
                GENERATED_AT_TIME,
                RefNodeFactory.toDateTime(value)));

    }

    public boolean shouldResolveOnMissingOnly() {
        return resolveOnMissingOnly;
    }

    public void setResolveOnMissingOnly(boolean resolveOnMissingOnly) {
        this.resolveOnMissingOnly = resolveOnMissingOnly;
    }

    private IRI findMostRecent(IRI obj) throws IOException {
        IRI existingId = findKey(Pair.of(Predicate.WAS_DERIVED_FROM, obj));

        if (existingId != null) {
            emitExistingVersion(existingId, Predicate.WAS_DERIVED_FROM, obj);
            existingId = findLastVersionId(existingId);
        }
        return existingId;
    }

    private IRI findLastVersionId(IRI existingId) throws IOException {
        IRI lastVersionId = existingId;
        IRI newerVersionId;
        while ((newerVersionId = findKey(Pair.of(Predicate.WAS_REVISION_OF, lastVersionId))) != null) {
            emitExistingVersion(newerVersionId, Predicate.WAS_REVISION_OF, lastVersionId);
            lastVersionId = newerVersionId;
        }
        return lastVersionId;
    }

    private void emitExistingVersion(IRI subj, IRI predicate, RDFTerm obj) throws IOException {
        IRI timeKey = findKey(Pair.of(subj, Predicate.GENERATED_AT_TIME));
        if (timeKey != null) {
            InputStream input = blobStore.get(timeKey);
            if (input != null) {
                emit(RefNodeFactory.toStatement(subj,
                        GENERATED_AT_TIME,
                        RefNodeFactory.toLiteral(IOUtils.toString(input, StandardCharsets.UTF_8))));
            }
        }
        emit(RefNodeFactory.toStatement(subj,
                predicate,
                obj));
    }


    @Override
    public void put(Pair<RDFTerm, RDFTerm> partialStatement, RDFTerm value) throws IOException {
        // write-once, read-many
        IRI key = calculateKeyFor(partialStatement);
        persistence.put(key.getIRIString(), value instanceof IRI ? ((IRI)value).getIRIString() : value.toString());
    }

    private IRI calculateKeyFor(Pair<RDFTerm, RDFTerm> unhashedKeyPair) {
        IRI left = hashRDFTerm(unhashedKeyPair.getLeft());
        IRI right = hashRDFTerm(unhashedKeyPair.getRight());
        return Hasher.calcSHA256(left.getIRIString() + right.getIRIString());
    }

    private IRI hashRDFTerm(RDFTerm left1) {
        return Hasher.calcSHA256(RDFUtil.getValueFor(left1));
    }

    @Override
    public IRI findKey(Pair<RDFTerm, RDFTerm> partialStatement) throws IOException {
        InputStream inputStream = persistence.get(calculateKeyFor(partialStatement).getIRIString());
        return inputStream == null
                ? null
                : RefNodeFactory.toIRI(URI.create(IOUtils.toString(inputStream, StandardCharsets.UTF_8)));
    }


    public Dereferencer getDereferencer() {
        return dereferencer;
    }

    public void setDereferencer(Dereferencer dereferencer) {
        this.dereferencer = dereferencer;
    }
}
