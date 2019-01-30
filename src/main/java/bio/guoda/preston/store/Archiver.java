package bio.guoda.preston.store;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.rdf.api.BlankNode;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Literal;
import org.apache.commons.rdf.api.Triple;
import bio.guoda.preston.cmd.CmdList;
import bio.guoda.preston.cmd.CrawlContext;
import bio.guoda.preston.process.StatementListener;
import bio.guoda.preston.process.StatementProcessor;

import java.io.IOException;
import java.io.InputStream;

import static bio.guoda.preston.RefNodeConstants.GENERATED_AT_TIME;
import static bio.guoda.preston.RefNodeConstants.HAS_PREVIOUS_VERSION;
import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeConstants.WAS_GENERATED_BY;
import static bio.guoda.preston.model.RefNodeFactory.getVersion;
import static bio.guoda.preston.model.RefNodeFactory.getVersionSource;
import static bio.guoda.preston.model.RefNodeFactory.toSkolemizedBlank;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;


public class Archiver extends StatementProcessor {
    private static Log LOG = LogFactory.getLog(CmdList.class);

    private final BlobStore blobStore;
    private final CrawlContext crawlContext;

    private Dereferencer dereferencer;

    private boolean resolveOnMissingOnly = false;

    private final StatementStore statementStore;

    public Archiver(BlobStore blobStore, Dereferencer dereferencer, StatementStore statementStore, CrawlContext crawlContext, StatementListener... listener) {
        super(listener);
        this.crawlContext = crawlContext;
        this.blobStore = blobStore;
        this.statementStore = statementStore;
        this.dereferencer = dereferencer;
    }

    public Archiver(BlobStore blobStore, Dereferencer dereferencer, StatementStore statementStore, CrawlContext crawlContext) {
        this(blobStore, dereferencer, statementStore, crawlContext, new StatementListener[]{});
    }

    StatementStore getStatementStore() {
        return this.statementStore;
    }

    @Override
    public void on(Triple statement) {
        try {
            BlankNodeOrIRI version = getVersion(statement);
            if (version instanceof BlankNode) {
                handleVersions(statement, (BlankNode) version);
            } else {
                emitExistingVersion(statement);

            }
        } catch (Throwable e) {
            LOG.warn("failed to handle [" + statement.toString() + "]", e);
        }

    }

    private void handleVersions(Triple statement, BlankNode blankVersion) throws IOException {
        IRI versionSource = getVersionSource(statement);
        IRI previousVersion = VersionUtil.findMostRecentVersion(versionSource, getStatementStore(), new VersionListener() {

            @Override
            public void onVersion(Triple statement) throws IOException {
                emitExistingVersion(statement);
            }
        });
        if (previousVersion == null || !shouldResolveOnMissingOnly()) {
            if (getDereferencer() != null) {
                IRI newVersion = null;
                try {
                    newVersion = dereference(versionSource);
                } catch (IOException e) {
                    LOG.warn("failed to dereference [" + versionSource.toString() + "]", e);
                } finally {
                    if (newVersion == null) {
                        newVersion = toSkolemizedBlank(blankVersion);
                    }
                    putVersion(versionSource, previousVersion, newVersion);
                }
            }
        }
    }

    private void putVersion(IRI versionSource, IRI previousVersion, BlankNodeOrIRI newVersion) throws IOException {
        if (null != previousVersion && !previousVersion.equals(newVersion)) {
            recordGenerationTime(newVersion);
            getStatementStore().put(Pair.of(HAS_PREVIOUS_VERSION, previousVersion), newVersion);
            emit(toStatement(newVersion, HAS_PREVIOUS_VERSION, previousVersion));

        } else if (null == previousVersion) {
            recordGenerationTime(newVersion);
            getStatementStore().put(Pair.of(versionSource, HAS_VERSION), newVersion);
            emit(toStatement(versionSource, HAS_VERSION, newVersion));
        }
    }

    private IRI dereference(IRI versionSource) throws IOException {
        InputStream data = getDereferencer().dereference(versionSource);
        return data == null ? null : getBlobStore().putBlob(data);
    }

    private void recordGenerationTime(BlankNodeOrIRI derivedSubject) throws IOException {
        Literal nowLiteral = VersionUtil.recordGenerationTimeFor(derivedSubject, getBlobStore(), getStatementStore());
        emit(toStatement(derivedSubject,
                GENERATED_AT_TIME,
                nowLiteral));

        emit(toStatement(derivedSubject,
                WAS_GENERATED_BY,
                crawlContext.getActivity()));
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

    private void emitExistingVersion(Triple statement) throws IOException {
        emitGenerationInfo(statement);
        emit(statement);
    }

    private void emitGenerationInfo(Triple statement) throws IOException {
        Triple statement1 = VersionUtil.generationTimeFor(statement.getSubject(), getStatementStore(), getBlobStore());
        if (statement1 != null) {
            emit(statement1);
        }
        IRI crawlActivityKey = getStatementStore().get(Pair.of(statement.getSubject(), WAS_GENERATED_BY));
        if (crawlActivityKey != null) {
            InputStream input = getBlobStore().get(crawlActivityKey);
            if (input != null) {
                emit(toStatement(statement.getSubject(),
                        WAS_GENERATED_BY,
                        crawlActivityKey));
            }
        }
    }

    public Dereferencer getDereferencer() {
        return dereferencer;
    }

    public void setDereferencer(Dereferencer dereferencer) {
        this.dereferencer = dereferencer;
    }

}
