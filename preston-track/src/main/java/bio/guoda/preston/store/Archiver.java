package bio.guoda.preston.store;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.cmd.ActivityContext;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.process.StatementsEmitter;
import bio.guoda.preston.process.StatementsListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.rdf.api.BlankNode;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Literal;
import org.apache.commons.rdf.api.Quad;

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

import static bio.guoda.preston.RefNodeConstants.*;
import static bio.guoda.preston.RefNodeFactory.getVersionSource;
import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toSkolemizedBlank;
import static bio.guoda.preston.RefNodeFactory.toStatement;


public class Archiver extends VersionProcessor {
    private static Logger LOG = LoggerFactory.getLogger(Archiver.class);

    private final ActivityContext activityCtx;

    private final Dereferencer<IRI> dereferencer;

    public Archiver(Dereferencer<IRI> dereferencer, ActivityContext activityCtx, StatementsListener... listener) {
        super(listener);
        this.activityCtx = activityCtx;
        this.dereferencer = dereferencer;
    }

    @Override
    void handleBlankVersion(Quad statement, BlankNode blankVersion) throws IOException {
        IRI versionSource = getVersionSource(statement);
        if (getDereferencer() != null) {
            IRI newVersion = null;
            try {
                newVersion = dereferencer.get(versionSource);
            } catch (IOException e) {
                LOG.warn("failed to dereference [" + versionSource.toString() + "]", e);
            } finally {
                if (newVersion == null) {
                    newVersion = toSkolemizedBlank(blankVersion);
                }
                putVersion(versionSource, newVersion, this, statement.getGraphName());
            }
        }
    }

    private void putVersion(IRI versionSource, BlankNodeOrIRI newVersion, StatementsEmitter emitter, Optional<BlankNodeOrIRI> sourceActivity) {
        Literal nowLiteral = RefNodeFactory.nowDateTimeLiteral();

        IRI downloadActivity = toIRI(UUID.randomUUID());
        emitter.emit(toStatement(
                downloadActivity,
                newVersion,
                WAS_GENERATED_BY,
                downloadActivity));
        emitter.emit(toStatement(
                downloadActivity,
                newVersion,
                QUALIFIED_GENERATION,
                downloadActivity));
        emitter.emit(toStatement(
                downloadActivity,
                downloadActivity,
                GENERATED_AT_TIME,
                nowLiteral));
        emitter.emit(toStatement(
                downloadActivity,
                downloadActivity,
                IS_A,
                GENERATION));
        sourceActivity.ifPresent(blankNodeOrIRI -> emitter.emit(toStatement(
                downloadActivity,
                downloadActivity,
                WAS_INFORMED_BY,
                blankNodeOrIRI)));
        emitter.emit(toStatement(
                downloadActivity,
                downloadActivity,
                USED,
                versionSource));
        emitter.emit(toStatement(downloadActivity, versionSource, HAS_VERSION, newVersion));
    }

    private Dereferencer<IRI> getDereferencer() {
        return dereferencer;
    }

}
