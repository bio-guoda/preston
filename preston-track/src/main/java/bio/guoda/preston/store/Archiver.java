package bio.guoda.preston.store;

import bio.guoda.preston.cmd.ActivityContext;
import bio.guoda.preston.process.ActivityUtil;
import bio.guoda.preston.process.StatementsListener;
import org.apache.commons.rdf.api.BlankNode;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static bio.guoda.preston.RefNodeFactory.getVersionSource;
import static bio.guoda.preston.RefNodeFactory.toSkolemizedBlank;


public class Archiver extends VersionProcessor {
    private static Logger LOG = LoggerFactory.getLogger(Archiver.class);

    private final ActivityContext activityCtx;

    private final Dereferencer<IRI> dereferencer;

    public Archiver(
            Dereferencer<IRI> dereferencer,
            ActivityContext activityCtx,
            StatementsListener... listener) {
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
                ActivityUtil.emitDownloadActivity(versionSource, newVersion, this, statement.getGraphName());
            }
        }
    }

    private Dereferencer<IRI> getDereferencer() {
        return dereferencer;
    }

}
