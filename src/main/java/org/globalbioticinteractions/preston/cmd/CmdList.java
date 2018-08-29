package org.globalbioticinteractions.preston.cmd;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.globalbioticinteractions.preston.RefNodeConstants;
import org.globalbioticinteractions.preston.Resources;
import org.globalbioticinteractions.preston.model.RefNodeRelation;
import org.globalbioticinteractions.preston.model.RefNodeString;
import org.globalbioticinteractions.preston.process.BlobStoreWriter;
import org.globalbioticinteractions.preston.process.RegistryReaderGBIF;
import org.globalbioticinteractions.preston.Seeds;
import org.globalbioticinteractions.preston.process.RefNodeListener;
import org.globalbioticinteractions.preston.process.RegistryReaderIDigBio;
import org.globalbioticinteractions.preston.process.RelationLogWriter;
import org.globalbioticinteractions.preston.store.AppendOnlyBlobStore;
import org.globalbioticinteractions.preston.store.AppendOnlyRelationStore;
import org.globalbioticinteractions.preston.store.FilePersistence;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

@Parameters(separators = "= ", commandDescription = "List Biodiversity Graph Nodes")
public class CmdList implements Runnable {

    @Parameter(names = {"-u", "--seed-uris"}, description = "[URIs to start crawl (aka seed URIs)]", validateWith = URIValidator.class)
    private List<String> seedUrls = new ArrayList<String>() {{
        add(Seeds.SEED_NODE_IDIGBIO.getLabel());
        add(Seeds.SEED_NODE_GBIF.getLabel());
    }};

    @Override
    public void run() {
        final List<RefNodeRelation> seeds = seedUrls.stream()
                .map(uriString -> {
                    RefNodeString refNodeRoot = RefNodeConstants.SEED_ROOT;
                    RefNodeString refNodeSeed = new RefNodeString(uriString);
                    return new RefNodeRelation(refNodeRoot, RefNodeConstants.SEED_OF, refNodeSeed);
                }).collect(Collectors.toList());

        final Queue<RefNodeRelation> refNodes =
                new ConcurrentLinkedQueue<RefNodeRelation>() {{
                    addAll(seeds);
                }};

        FilePersistence persistence = new FilePersistence();
        AppendOnlyBlobStore blobStore = new AppendOnlyBlobStore(persistence);
        AppendOnlyRelationStore relationStore = new AppendOnlyRelationStore(blobStore, persistence, Resources::asInputStream);

        final RefNodeListener listener = new BlobStoreWriter(blobStore, relationStore,
                new RegistryReaderIDigBio(refNodes::add),
                new RegistryReaderGBIF(refNodes::add),
                new RelationLogWriter());

        while (!refNodes.isEmpty()) {
            listener.on(refNodes.poll());
        }

    }

}
