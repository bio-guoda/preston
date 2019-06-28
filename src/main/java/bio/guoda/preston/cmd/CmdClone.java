package bio.guoda.preston.cmd;

import com.beust.jcommander.Parameters;
import org.apache.commons.lang3.time.StopWatch;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import static bio.guoda.preston.cmd.ReplayUtil.attemptReplay;

@Parameters(separators = "= ", commandDescription = "Clone biodiversity dataset graph")
public class CmdClone extends LoggingPersisting implements Runnable {

    @Override
    public void run() {
        CloneUtil.clone(getKeyValueStore());
    }

}
