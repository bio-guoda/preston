package org.globalbioticinteractions.preston.cmd;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.globalbioticinteractions.preston.RefNodeConstants;
import org.globalbioticinteractions.preston.Resources;
import org.globalbioticinteractions.preston.model.RefStatement;
import org.globalbioticinteractions.preston.model.RefNodeString;
import org.globalbioticinteractions.preston.process.ContentResolver;
import org.globalbioticinteractions.preston.process.RegistryReaderGBIF;
import org.globalbioticinteractions.preston.Seeds;
import org.globalbioticinteractions.preston.process.RefNodeListener;
import org.globalbioticinteractions.preston.process.RegistryReaderIDigBio;
import org.globalbioticinteractions.preston.process.StatementLog;
import org.globalbioticinteractions.preston.store.AppendOnlyBlobStore;
import org.globalbioticinteractions.preston.store.AppendOnlyStatementStore;
import org.globalbioticinteractions.preston.store.FilePersistence;
import org.globalbioticinteractions.preston.store.StatementStore;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

@Parameters(separators = "= ", commandDescription = "List Biodiversity Graph Nodes")
public class CmdList implements Runnable {

    @Parameter(names = {"-u", "--seed-uris"}, description = "[URIs to start crawl (aka seed URIs)]", validateWith = URIValidator.class)
    private List<String> seedUrls = new ArrayList<String>() {{
        add(Seeds.SEED_NODE_GBIF.getLabel());
        add(Seeds.SEED_NODE_IDIGBIO.getLabel());
    }};

    @Parameter(names = {"-o", "--offline"}, description = "lists nodes in biodiversity graph using a local archive")
    private boolean offline = false;

    @Override
    public void run() {
        final List<RefStatement> seeds = seedUrls.stream()
                .map(uriString -> {
                    RefNodeString refNodeRoot = RefNodeConstants.SEED_ROOT;
                    RefNodeString refNodeSeed = new RefNodeString(uriString);
                    return new RefStatement(refNodeRoot, RefNodeConstants.SEED_OF, refNodeSeed);
                }).collect(Collectors.toList());

        final Queue<RefStatement> statements =
                new ConcurrentLinkedQueue<RefStatement>() {{
                    addAll(seeds);
                }};

        FilePersistence persistence = new FilePersistence();
        AppendOnlyBlobStore blobStore = new AppendOnlyBlobStore(persistence);

        StatementStore<URI> statementStore = offline
                ? createOfflineStatementStore(persistence, blobStore)
                : createOnlineStatementStore(persistence, blobStore);

        final RefNodeListener listener = new ContentResolver(blobStore, statementStore,
                new RegistryReaderIDigBio(statements::add),
                new RegistryReaderGBIF(statements::add),
                new StatementLog());

        while (!statements.isEmpty()) {
            listener.on(statements.poll());
        }

    }

    private AppendOnlyStatementStore createOfflineStatementStore(FilePersistence persistence, AppendOnlyBlobStore blobStore) {
        return new AppendOnlyStatementStore(blobStore, persistence, Resources::asInputStream) {

            @Override
            public void put(Pair<URI, URI> partialStatement, URI value) throws IOException {

            }

            @Override
            public void put(Triple<URI, URI, URI> statement) throws IOException {

            }

        };
    }

   private AppendOnlyStatementStore createOnlineStatementStore(FilePersistence persistence, AppendOnlyBlobStore blobStore) {
        return new AppendOnlyStatementStore(blobStore, persistence, Resources::asInputStream);
    }

}
