package bio.guoda.preston.cmd;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

@Parameters(separators = "= ", commandDescription = "update biodiversity graph")
public class CmdUpdate extends CmdCrawl {

    @Parameter(names = {"-i", "--incremental",}, description = "resume unfinished update")
    private boolean incremental = false;


    @Override
    public CrawlMode getCrawlMode() {
        return incremental ? CrawlMode.resume : CrawlMode.restart;
    }
}
