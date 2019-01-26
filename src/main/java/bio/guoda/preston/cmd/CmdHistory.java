package bio.guoda.preston.cmd;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.rdf.api.Triple;
import bio.guoda.preston.StatementLogFactory;
import bio.guoda.preston.model.RefNodeFactory;
import bio.guoda.preston.process.StatementListener;
import bio.guoda.preston.store.AppendOnlyBlobStore;
import bio.guoda.preston.store.BlobStore;
import bio.guoda.preston.store.StatementStore;
import bio.guoda.preston.store.StatementStoreImpl;
import bio.guoda.preston.store.VersionListener;
import bio.guoda.preston.store.VersionUtil;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static bio.guoda.preston.RefNodeConstants.ARCHIVE_COLLECTION;
import static bio.guoda.preston.model.RefNodeFactory.toBlank;

@Parameters(separators = "= ", commandDescription = "show history of biodiversity resource")
public class CmdHistory extends LoggingPersisting implements Runnable {

    private static final Log LOG = LogFactory.getLog(CmdHistory.class);

    @Parameter(description = "biodiversity resource locator", validateWith = IRIValidator.class)
    private String biodiversityNode = ARCHIVE_COLLECTION.toString();


    @Override
    public void run() {
        StatementListener logger = StatementLogFactory.createLogger(getLogMode());
        StatementStore statementStore = new StatementStoreImpl(getDatasetRelationsStore());
        BlobStore blobStore = new AppendOnlyBlobStore(getBlobPersistence());
        AtomicBoolean gotNone = new AtomicBoolean(true);
        try {
            VersionUtil.findMostRecentVersion(RefNodeFactory.toIRI(biodiversityNode), statementStore, new VersionListener() {
                @Override
                public void onVersion(Triple statement) throws IOException {
                    gotNone.set(false);
                    Triple generationStatement = VersionUtil.generationTimeFor(statement.getSubject(), statementStore, blobStore);
                    if (generationStatement != null) {
                        logger.on(generationStatement);
                    }
                    logger.on(statement);
                }
            });
        } catch (IOException e) {
            throw new RuntimeException("failed to get versions");
        }

        if (gotNone.get()) {
            LOG.warn("No history found for [" + biodiversityNode + "]. Suggest to update first.");
        }
    }
}
