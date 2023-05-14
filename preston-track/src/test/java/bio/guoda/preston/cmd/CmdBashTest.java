package bio.guoda.preston.cmd;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

public class CmdBashTest {

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

}