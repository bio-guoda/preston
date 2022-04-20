package bio.guoda.preston.cmd;

import org.hamcrest.core.Is;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

import static junit.framework.TestCase.assertNotNull;
import static org.hamcrest.MatcherAssert.assertThat;

public class CmdCopyToTest {

    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    @Test
    public void trackAndCopy() throws URISyntaxException, IOException {
        URL resource = getClass().getResource("/bio/guoda/preston/cmd/copy-test-data/2a/5d/2a5de79372318317a382ea9a2cef069780b852b01210ef59e06b640a3539cb5a");
        assertNotNull(resource);
        File file = new File(resource.toURI());
        File dataDir = file.getParentFile().getParentFile().getParentFile();

        File copyTo = tmpDir.newFolder();
        assertCopy(dataDir, copyTo);
        File copyToAgain = tmpDir.newFolder();
        assertCopy(copyTo, copyToAgain);
    }

    private void assertCopy(File dataDir, File targetDir) throws IOException {
        CmdCopyTo cmdCopyTo = new CmdCopyTo();
        cmdCopyTo.setLocalDataDir(dataDir.getAbsolutePath());
        cmdCopyTo.setTargetDir(targetDir.getAbsolutePath());
        cmdCopyTo.setPathPattern(HashPathPattern.directoryDepth0);
        cmdCopyTo.setArchiveType(ArchiveType.data_prov_provindex);

        assertThat(targetDir.list(), Is.is(new String[] {}));

        cmdCopyTo.run();

        assertThat(targetDir.list(), Is.is(new String[] { "2a", "42", "ea"}));
    }

}