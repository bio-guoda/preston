package bio.guoda.preston.cmd;

import bio.guoda.preston.RefNodeFactory;
import org.apache.commons.io.IOUtils;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

public class CmdBashIT {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    // test currently only works on Posix compatible systems
    @Test
    public void ls() {
        CmdBash cmdBash = new CmdBash();
        cmdBash.setInputStream(IOUtils.toInputStream("ls", StandardCharsets.UTF_8));

        ByteArrayOutputStream boas = new ByteArrayOutputStream();
        cmdBash.setOutputStream(boas);

        cmdBash.run();

        assertThat(new String(boas.toByteArray(), StandardCharsets.UTF_8),
                containsString("text/x-shellscript")
        );
    }

    // test currently only works on Posix compatible systems
    @Test
    public void echo() {
        CmdBash cmdBash = new CmdBash();

        cmdBash.setInputStream(IOUtils.toInputStream("ls -1", StandardCharsets.UTF_8));

        ByteArrayOutputStream boas = new ByteArrayOutputStream();
        cmdBash.setOutputStream(boas);

        cmdBash.run();

        assertThat(new String(boas.toByteArray(), StandardCharsets.UTF_8),
                containsString("text/x-shellscript")
        );
    }

    // this test doesn't finish yet, because it waits for stdin to complete
    @Test
    public void lsViaC() throws URISyntaxException {
        CmdTrack cmdTrack = new CmdTrack();
        cmdTrack.setLocalDataDir(folder.getRoot().getAbsolutePath());

        cmdTrack.setIRIs(Collections.singletonList(RefNodeFactory.toIRI(getClass().getResource("cat.sh").toURI())));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        cmdTrack.setOutputStream(baos);
        cmdTrack.run();


        CmdBash cmdBash = new CmdBash();
        cmdBash.setLocalDataDir(folder.getRoot().getAbsolutePath());
        cmdBash.setCommandsContentId(RefNodeFactory.toIRI("hash://sha256/77af778b51abd4a3c51c5ddd97204a9c3ae614ebccb75a606c3b6865aed6744e"));
        cmdBash.setInputStream(IOUtils.toInputStream("ls", StandardCharsets.UTF_8));

        ByteArrayOutputStream boas = new ByteArrayOutputStream();
        cmdBash.setOutputStream(boas);

        cmdBash.run();

        assertThat(new String(boas.toByteArray(), StandardCharsets.UTF_8),
                containsString("text/x-shellscript")
        );
    }


}