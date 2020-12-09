package bio.guoda.preston.cmd;

import bio.guoda.preston.process.BlobStoreReadOnly;
import bio.guoda.preston.process.StatementsListenerAdapter;
import bio.guoda.preston.store.VersionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

import java.io.IOException;
import java.io.InputStream;

public class VersionRetriever extends StatementsListenerAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(VersionRetriever.class);

    private final BlobStoreReadOnly blobStore;

    public VersionRetriever(BlobStoreReadOnly blobStore) {
        this.blobStore = blobStore;
    }

    @Override
    public void on(Quad statement) {
        IRI mostRecentVersion = VersionUtil.mostRecentVersionForStatement(statement);
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

}
