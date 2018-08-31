package org.globalbioticinteractions.preston.cmd;

import com.beust.jcommander.Parameters;

@Parameters(separators = "= ", commandDescription = "list biodiversity graph")
public class CmdList extends CmdCrawl {

    @Override
    public CrawlMode getCrawlMode() {
        return CrawlMode.replay;
    }
}
