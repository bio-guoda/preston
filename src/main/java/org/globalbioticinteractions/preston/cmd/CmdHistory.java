package org.globalbioticinteractions.preston.cmd;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDFTerm;
import org.apache.commons.rdf.api.Triple;
import org.globalbioticinteractions.preston.StatementLogFactory;
import org.globalbioticinteractions.preston.model.RefNodeFactory;
import org.globalbioticinteractions.preston.process.StatementListener;
import org.globalbioticinteractions.preston.process.VersionLogger;
import org.globalbioticinteractions.preston.store.AppendOnlyBlobStore;
import org.globalbioticinteractions.preston.store.Archiver;
import org.globalbioticinteractions.preston.store.BlobStore;
import org.globalbioticinteractions.preston.store.StatementStore;
import org.globalbioticinteractions.preston.store.StatementStoreImpl;
import org.globalbioticinteractions.preston.store.VersionListener;
import org.globalbioticinteractions.preston.store.VersionUtil;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.globalbioticinteractions.preston.RefNodeConstants.ARCHIVE;
import static org.globalbioticinteractions.preston.RefNodeConstants.ARCHIVE_COLLECTION;
import static org.globalbioticinteractions.preston.RefNodeConstants.GENERATED_AT_TIME;
import static org.globalbioticinteractions.preston.RefNodeConstants.HAS_VERSION;
import static org.globalbioticinteractions.preston.model.RefNodeFactory.toBlank;
import static org.globalbioticinteractions.preston.model.RefNodeFactory.toStatement;

@Parameters(separators = "= ", commandDescription = "show history of biodiversity resource")
public class CmdHistory extends Persisting implements Runnable {

    private static final Log LOG = LogFactory.getLog(CmdHistory.class);

    @Parameter(description = "biodiversity resource locator", validateWith = IRIValidator.class)
    private String biodiversityNode = ARCHIVE_COLLECTION.toString();


    @Override
    public void run() {
        StatementListener logger = StatementLogFactory.createLogger(getLogMode());
        StatementStore statementStore = new StatementStoreImpl(getStatementPersistence());
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
