package bio.guoda.preston.cmd;

import bio.guoda.preston.store.AppendOnlyBlobStore;
import bio.guoda.preston.store.CopyingKeyValueStore;
import bio.guoda.preston.store.KeyValueStore;
import bio.guoda.preston.store.StatementStore;
import bio.guoda.preston.store.StatementStoreImpl;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import static bio.guoda.preston.cmd.ReplayUtil.attemptReplay;
import static bio.guoda.preston.model.RefNodeFactory.toBlank;

@Parameters(separators = "= ", commandDescription = "Copy biodiversity dataset graph")
public class CmdCopy extends LoggingPersisting implements Runnable {

    private static final Log LOG = LogFactory.getLog(CmdCopy.class);

    @Parameter(description = "[target directory]")
    private String targetDir;

    @Override
    public void run() {
        KeyValueStore copyingKeyValueStore = new CopyingKeyValueStore("data", targetDir, "tmp");
        attemptReplay(
                new AppendOnlyBlobStore(copyingKeyValueStore)
                , new StatementStoreImpl(copyingKeyValueStore)
                , getLogMode());
    }

}
