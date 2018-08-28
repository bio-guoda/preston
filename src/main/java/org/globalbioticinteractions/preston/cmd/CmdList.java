package org.globalbioticinteractions.preston.cmd;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.globalbioticinteractions.preston.RefNodeConstants;
import org.globalbioticinteractions.preston.model.RefNodeRelation;
import org.globalbioticinteractions.preston.model.RefNodeString;
import org.globalbioticinteractions.preston.model.RefNodeType;
import org.globalbioticinteractions.preston.process.BlobStoreWriter;
import org.globalbioticinteractions.preston.process.RegistryReaderGBIF;
import org.globalbioticinteractions.preston.Seeds;
import org.globalbioticinteractions.preston.model.RefNode;
import org.globalbioticinteractions.preston.process.RefNodeListener;
import org.globalbioticinteractions.preston.process.RegistryReaderIDigBio;
import org.globalbioticinteractions.preston.process.RelationLogWriter;

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
        final List<RefNode> seeds = seedUrls.stream()
                .map(uriString -> {
                    RefNodeString refNodeRoot = RefNodeConstants.SEED_ROOT;
                    RefNodeString refNodeSeed = new RefNodeString(RefNodeType.URI, uriString);
                    return new RefNodeRelation(refNodeRoot, RefNodeConstants.SEED_OF, refNodeSeed);
                }).collect(Collectors.toList());

        final Queue<RefNode> refNodes =
                new ConcurrentLinkedQueue<RefNode>() {{
                    addAll(seeds);
                }};

        final RefNodeListener listener = new BlobStoreWriter(
                new RegistryReaderIDigBio(refNodes::add),
                new RegistryReaderGBIF(refNodes::add),
                new RelationLogWriter());

        while (!refNodes.isEmpty()) {
            listener.on(refNodes.poll());
        }

    }

}
