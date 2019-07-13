package bio.guoda.preston.store;

import bio.guoda.preston.cmd.ActivityContext;
import bio.guoda.preston.model.RefNodeFactory;
import bio.guoda.preston.process.StatementListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.rdf.api.BlankNode;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Literal;
import org.apache.commons.rdf.api.Triple;

import java.io.IOException;

import static bio.guoda.preston.RefNodeConstants.GENERATED_AT_TIME;
import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeConstants.WAS_GENERATED_BY;
import static bio.guoda.preston.model.RefNodeFactory.getVersionSource;
import static bio.guoda.preston.model.RefNodeFactory.toSkolemizedBlank;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;


public class Archiver extends VersionProcessor {
    private static Log LOG = LogFactory.getLog(Archiver.class);

    private final ActivityContext activityCtx;

    private Dereferencer<IRI> dereferencer;

    public Archiver(Dereferencer<IRI> dereferencer, ActivityContext activityCtx, StatementListener... listener) {
        super(listener);
        this.activityCtx = activityCtx;
        this.dereferencer = dereferencer;
    }

    @Override
    void handleBlankVersion(Triple statement, BlankNode blankVersion) throws IOException {
        IRI versionSource = getVersionSource(statement);
        if (getDereferencer() != null) {
            IRI newVersion = null;
            try {
                newVersion = dereferencer.dereference(versionSource);
            } catch (IOException e) {
                LOG.warn("failed to dereference [" + versionSource.toString() + "]", e);
            } finally {
                if (newVersion == null) {
                    newVersion = toSkolemizedBlank(blankVersion);
                }
                putVersion(versionSource, newVersion);
            }
        }
    }

    private void putVersion(IRI versionSource, BlankNodeOrIRI newVersion) throws IOException {
        emitGenerationTime(newVersion);
        emit(toStatement(versionSource, HAS_VERSION, newVersion));
    }

    private void emitGenerationTime(BlankNodeOrIRI derivedSubject) throws IOException {
        Literal nowLiteral = RefNodeFactory.nowDateTimeLiteral();
        emit(toStatement(derivedSubject,
                GENERATED_AT_TIME,
                nowLiteral));

        if (activityCtx != null) {
            emit(toStatement(derivedSubject,
                    WAS_GENERATED_BY,
                    activityCtx.getActivity()));
        }
    }

    private Dereferencer<IRI> getDereferencer() {
        return dereferencer;
    }

}
