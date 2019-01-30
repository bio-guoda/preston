package bio.guoda.preston.cmd;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.model.RefNodeFactory;
import bio.guoda.preston.process.BlobStoreReadOnly;
import bio.guoda.preston.process.StatementListener;
import bio.guoda.preston.store.BlobStore;
import bio.guoda.preston.store.VersionUtil;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDFTerm;
import org.apache.commons.rdf.api.Triple;
import sun.misc.Version;

import java.io.IOException;
import java.io.InputStream;

public class VersionRetriever implements StatementListener {
    private static final Log LOG = LogFactory.getLog(VersionRetriever.class);

    private final BlobStoreReadOnly blobStore;

    public VersionRetriever(BlobStoreReadOnly blobStore) {
        this.blobStore = blobStore;
    }

    @Override
    public void on(Triple statement) {
        IRI mostRecentVersion = mostRecentVersionForStatement(statement);
        if (mostRecentVersion != null) {
            touchMostRecentVersion(mostRecentVersion);
        }
    }

    private void touchMostRecentVersion(IRI mostRecentVersion) {
        try {
            InputStream inputStream = blobStore.get(mostRecentVersion);
            if (inputStream != null) {
                inputStream.close();
            }
        } catch (IOException e) {
            LOG.warn("failed to access [" + mostRecentVersion.getIRIString() + "]");
        }
    }

    public static IRI mostRecentVersionForStatement(Triple statement) {
        IRI mostRecentVersion = null;
        RDFTerm mostRecentTerm = null;
        if (statement.getPredicate().equals(RefNodeConstants.HAS_PREVIOUS_VERSION)) {
            mostRecentTerm = statement.getSubject();
        } else if (statement.getPredicate().equals(RefNodeConstants.HAS_VERSION)) {
            mostRecentTerm = statement.getObject();
        }
        if (mostRecentTerm instanceof IRI) {
            IRI mostRecentIRI = (IRI) mostRecentTerm;
            if (!RefNodeFactory.isBlankOrSkolemizedBlank(mostRecentIRI)) {
                mostRecentVersion = mostRecentIRI;
            }
        }
        return mostRecentVersion;
    }
}
