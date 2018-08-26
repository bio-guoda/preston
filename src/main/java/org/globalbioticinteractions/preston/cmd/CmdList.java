package org.globalbioticinteractions.preston.cmd;

import com.beust.jcommander.Parameters;
import org.globalbioticinteractions.preston.process.BlobStoreWriter;
import org.globalbioticinteractions.preston.process.GBIFRegistryReader;
import org.globalbioticinteractions.preston.process.LogWriter;
import org.globalbioticinteractions.preston.Seeds;
import org.globalbioticinteractions.preston.model.RefNode;
import org.globalbioticinteractions.preston.process.RefNodeListener;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

@Parameters(separators = "= ", commandDescription = "List Biodiversity Graph Nodes")
public class CmdList implements Runnable {


    @Override
    public void run() {
        final Queue<RefNode> refNodes = new ConcurrentLinkedQueue<>();
        refNodes.add(Seeds.SEED_NODE_GBIF);

        final RefNodeListener listener = new BlobStoreWriter(
                new GBIFRegistryReader(refNodes::add),
                new LogWriter());

        while(!refNodes.isEmpty()) {
            listener.on(refNodes.poll());
        }

    }

}
