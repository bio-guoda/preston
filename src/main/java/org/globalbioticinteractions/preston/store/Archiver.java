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
import org.globalbioticinteractions.preston.cmd.CmdList;
import org.globalbioticinteractions.preston.model.RefNodeFactory;
import org.globalbioticinteractions.preston.process.StatementListener;
import org.globalbioticinteractions.preston.process.StatementProcessor;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.globalbioticinteractions.preston.RefNodeConstants.GENERATED_AT_TIME;
import static org.globalbioticinteractions.preston.RefNodeConstants.HAS_PREVIOUS_VERSION;
import static org.globalbioticinteractions.preston.RefNodeConstants.HAS_VERSION;
import static org.globalbioticinteractions.preston.RefNodeConstants.WAS_DERIVED_FROM;
import static org.globalbioticinteractions.preston.RefNodeConstants.WAS_REVISION_OF;


public class Archiver extends StatementProcessor {
    private static Log LOG = LogFactory.getLog(CmdList.class);

    private BlobStore blobStore;

    private Dereferencer dereferencer;

    private boolean resolveOnMissingOnly = false;

    private final StatementStore statementStore;

    public Archiver(BlobStore blobStore, Dereferencer dereferencer, StatementStore statementStore, StatementListener... listener) {
        super(listener);
        this.blobStore = blobStore;
        this.statementStore = statementStore;
        this.dereferencer = dereferencer;
    }

    public Archiver(BlobStore blobStore, Dereferencer dereferencer, StatementStoreImpl statementStore) {
        this.blobStore = blobStore;
        this.statementStore = statementStore;
        this.dereferencer = dereferencer;
    }

    StatementStore getStatementStore() {
        return this.statementStore;
    }

    @Override
    public void on(Triple statement) {
        try {
            BlankNodeOrIRI version = RefNodeFactory.getVersion(statement);
            if (version == null || !(version instanceof BlankNode)) {
                emit(statement);
            } else {
                handleVersion(statement, (BlankNode) version);
            }
        } catch (Throwable e) {
            LOG.warn("failed to handle [" + statement.toString() + "]", e);
        }

    }

    private void handleVersion(Triple statement, BlankNode blankVersion) throws IOException {
        IRI versionSource = RefNodeFactory.getVersionSource(statement);
        IRI previousVersion = findMostRecentVersion(versionSource);
        if (previousVersion == null || !shouldResolveOnMissingOnly()) {
            if (getDereferencer() != null) {
                IRI newVersion = null;
                try {
                    newVersion = dereference(versionSource);
                } catch (IOException e) {
                    LOG.warn("failed to dereference [" + versionSource.toString() + "]", e);
                } finally {
                    if (newVersion == null) {
                        newVersion = RefNodeFactory.toSkolemizedBlank(blankVersion);
                    }
                    putVersion(versionSource, previousVersion, newVersion);
                }
            }
        }
    }

    private void putVersion(IRI versionSource, IRI previousVersion, BlankNodeOrIRI newVersion) throws IOException {
        if (null != previousVersion && !previousVersion.equals(newVersion)) {
            recordGenerationTime(newVersion);
            getStatementStore().put(Pair.of(WAS_REVISION_OF, previousVersion), newVersion);
            Triple of = RefNodeFactory.toStatement(newVersion, WAS_REVISION_OF, previousVersion);
            emit(of);

        } else if (null == previousVersion) {
            recordGenerationTime(newVersion);
            getStatementStore().put(Pair.of(WAS_DERIVED_FROM, versionSource), newVersion);
            emit(RefNodeFactory.toStatement(newVersion, WAS_DERIVED_FROM, versionSource));
        }
    }

    private IRI dereference(IRI versionSource) throws IOException {
        InputStream data = getDereferencer().dereference(versionSource);
        return getBlobStore().putBlob(data);
    }

    private void recordGenerationTime(BlankNodeOrIRI derivedSubject) throws IOException {
        String value = RefNodeFactory.toDateTime(DateUtil.now()).getLexicalForm();
        getBlobStore().putBlob(IOUtils.toInputStream(value, StandardCharsets.UTF_8));
        IRI value1 = Hasher.calcSHA256(value);

        Pair<RDFTerm, RDFTerm> of = Pair.of(derivedSubject, GENERATED_AT_TIME);
        getStatementStore().put(of, value1);
        emit(RefNodeFactory.toStatement(derivedSubject,
                GENERATED_AT_TIME,
                RefNodeFactory.toDateTime(value)));

    }

    private BlobStore getBlobStore() {
        return blobStore;
    }

    public boolean shouldResolveOnMissingOnly() {
        return resolveOnMissingOnly;
    }

    public void setResolveOnMissingOnly(boolean resolveOnMissingOnly) {
        this.resolveOnMissingOnly = resolveOnMissingOnly;
    }

    private IRI findMostRecentVersion(IRI versionSource) throws IOException {
        IRI mostRecentVersion = getStatementStore().get(Pair.of(versionSource, HAS_VERSION));
        if (mostRecentVersion == null) {
            mostRecentVersion = getStatementStore().get(Pair.of(WAS_DERIVED_FROM, versionSource));
            if (mostRecentVersion != null) {
                // migrate
                Pair<RDFTerm, RDFTerm> query = Pair.of(versionSource, HAS_VERSION);
                if (getStatementStore().get(query) == null) {
                    getStatementStore().put(query, mostRecentVersion);
                    recordTimeFor(mostRecentVersion);
                }
            }
        }

        if (mostRecentVersion != null) {
            emitExistingVersion(versionSource, HAS_VERSION, mostRecentVersion);

            // migrate
            migrateVersions(mostRecentVersion);
            mostRecentVersion = findLastVersion(mostRecentVersion);
        }
        return mostRecentVersion;
    }

    private IRI findLastVersion(IRI existingId) throws IOException {
        IRI lastVersionId = existingId;
        IRI newerVersionId;
        while ((newerVersionId = getStatementStore().get(Pair.of(HAS_PREVIOUS_VERSION, lastVersionId))) != null) {
            emitExistingVersion(newerVersionId, HAS_PREVIOUS_VERSION, lastVersionId);
            lastVersionId = newerVersionId;
        }
        return lastVersionId;
    }

    private void migrateVersions(IRI existingId) throws IOException {
        IRI lastVersionId = existingId;
        IRI newerVersionId;
        while ((newerVersionId = getStatementStore().get(Pair.of(WAS_REVISION_OF, lastVersionId))) != null) {
            Pair<RDFTerm, RDFTerm> query = Pair.of(HAS_PREVIOUS_VERSION, lastVersionId);
            if (getStatementStore().get(query) == null ) {
                getStatementStore().put(query, newerVersionId);
                recordTimeFor(newerVersionId);
            }
            lastVersionId = newerVersionId;
        }
    }

    private void recordTimeFor(IRI newerVersionId) throws IOException {
        IRI timeKey = getStatementStore().get(Pair.of(newerVersionId, GENERATED_AT_TIME));
        if (timeKey != null) {
            getStatementStore().put(Pair.of(newerVersionId, GENERATED_AT_TIME), timeKey);
        }
    }

    private void emitExistingVersion(IRI subj, IRI predicate, RDFTerm obj) throws IOException {
        IRI timeKey = getStatementStore().get(Pair.of(subj, GENERATED_AT_TIME));
        if (timeKey != null) {
            InputStream input = getBlobStore().get(timeKey);
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




    public Dereferencer getDereferencer() {
        return dereferencer;
    }

    public void setDereferencer(Dereferencer dereferencer) {
        this.dereferencer = dereferencer;
    }

}
