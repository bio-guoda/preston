package bio.guoda.preston.cmd;

import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

public class CmdBashTest {

    @Test
    public void ls() {
        CmdBash cmdBash = new CmdBash();
        cmdBash.setScript("ls");

        ByteArrayOutputStream boas = new ByteArrayOutputStream();
        cmdBash.setOutputStream(boas);

        cmdBash.run();

        assertThat(new String(boas.toByteArray(), StandardCharsets.UTF_8),
                containsString("text/x-shellscript")
        );
    }

}