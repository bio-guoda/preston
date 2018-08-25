package org.globalbioticinteractions.preston.cmd;

import com.beust.jcommander.Parameters;
import org.globalbioticinteractions.preston.process.Caching;
import org.globalbioticinteractions.preston.process.GBIFRegistry;
import org.globalbioticinteractions.preston.process.Logging;
import org.globalbioticinteractions.preston.Seeds;
import org.globalbioticinteractions.preston.model.RefNode;
import org.globalbioticinteractions.preston.process.RefNodeListener;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

@Parameters(separators = "= ", commandDescription = "Show Version")
public class CmdList implements Runnable {


    @Override
    public void run() {
        final Queue<RefNode> refNodes = new ConcurrentLinkedQueue<>();
        refNodes.add(Seeds.SEED_NODE_GBIF);

        final RefNodeListener listener = new Caching(
                new GBIFRegistry(refNodes::add),
                new Logging());

        while(!refNodes.isEmpty()) {
            listener.on(refNodes.poll());
        }

    }

}
