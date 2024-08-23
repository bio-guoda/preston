package bio.guoda.preston.cmd;

import bio.guoda.preston.Preston;
import org.apache.commons.io.IOUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNot.not;

public class PrestonIT {

    @Rule

    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void history() {
        Preston.run(new String[]{
                "history",
                "--remote",
                "https://deeplinker.bio/"
        });
    }

    @Test
    public void trackWithCustomMessage() throws IOException {
        // https://github.com/bio-guoda/preston/issues/300
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(outputStream);
        System.setOut(printStream);
        String customMsg = "this is a custom message";
        Preston.run(new String[]{
                "track",
                "-m",
                customMsg,
                "--data-dir",
                folder.newFolder("data-dir").getAbsolutePath(),
                "https://example.org"
        });

        assertThat(new String(outputStream.toByteArray(), StandardCharsets.UTF_8), containsString(customMsg));
    }

    @Test
    public void trackWithNoCustomMessage() throws IOException {
        // https://github.com/bio-guoda/preston/issues/300
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(outputStream);
        System.setOut(printStream);
        String customMsg = "this is a custom message";
        Preston.run(new String[]{
                "track",
                "--data-dir",
                folder.newFolder("data-dir").getAbsolutePath(),
                "https://example.org"
        });

        assertThat(new String(outputStream.toByteArray(), StandardCharsets.UTF_8), not(containsString(customMsg)));
    }

    @Test
    public void catDataOne() {
        Preston.run(new String[]{
                "cat",
                "--algo",
                "md5",
                "--remote",
                "https://dataone.org",
                "hash://md5/e27c99a7f701dab97b7d09c467acf468"
        });
    }

}