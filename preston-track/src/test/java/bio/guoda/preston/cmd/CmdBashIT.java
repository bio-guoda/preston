package bio.guoda.preston.cmd;

import bio.guoda.preston.HashType;
import bio.guoda.preston.Hasher;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.util.UUIDUtil;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.IsNot.not;

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

    @Test
    public void lsViaCByContentId() throws URISyntaxException, IOException {
        trackScript();

        CmdBash cmdBash = new CmdBash();
        cmdBash.setDataDir(folder.getRoot().getAbsolutePath());
        cmdBash.setCommandsContentId(RefNodeFactory.toIRI("hash://sha256/77af778b51abd4a3c51c5ddd97204a9c3ae614ebccb75a606c3b6865aed6744e"));

        File source = folder.newFile("stdinstub");
        try (FileOutputStream os = new FileOutputStream(source)) {
            IOUtils.write("ls", os, StandardCharsets.UTF_8);
        }
        cmdBash.setSource(source);

        ByteArrayOutputStream boas = new ByteArrayOutputStream();
        cmdBash.setOutputStream(boas);

        cmdBash.run();

        assertThat(new String(boas.toByteArray(), StandardCharsets.UTF_8),
                containsString("text/x-shellscript")
        );
    }

    @Test
    public void lsViaCByAlias() throws URISyntaxException, IOException {
        ByteArrayOutputStream baos = trackScript();

        String lastLine = getLastLine(baos);

        Matcher matcher = Pattern.compile(UUIDUtil.UUID_PATTERN_PART).matcher(lastLine);
        assertTrue(matcher.find());
        String group = matcher.group(0);

        IRI uuidAlias = RefNodeFactory.toIRI(UUID.fromString(group));

        CmdBash cmdBash = new CmdBash();
        cmdBash.setDataDir(folder.getRoot().getAbsolutePath());
        cmdBash.setCommandsContentId(uuidAlias);

        ByteArrayOutputStream boas = new ByteArrayOutputStream();
        cmdBash.setOutputStream(boas);

        File source = folder.newFile("stdinstub");

        String content = "foo";
        try (FileOutputStream fos = new FileOutputStream(source)) {
            IOUtils.write(content, fos, StandardCharsets.UTF_8);
        }
        cmdBash.setSource(source);
        cmdBash.run();

        String bashLastLine = getLastLine(boas);

        assertThat(bashLastLine, not(containsString("well-known")));

        IRI contentId = Hasher.calcHashIRI(content, HashType.sha256);
        assertThat(bashLastLine, containsString(" <http://purl.org/pav/hasVersion> " + contentId));


        assertThat(new String(boas.toByteArray(), StandardCharsets.UTF_8),
                containsString("text/x-shellscript")
        );
    }

    private ByteArrayOutputStream trackScript() throws URISyntaxException {
        CmdTrack cmdTrack = new CmdTrack();
        cmdTrack.setDataDir(folder.getRoot().getAbsolutePath());

        cmdTrack.setIRIs(Collections.singletonList(RefNodeFactory.toIRI(getClass().getResource("cat.sh").toURI())));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        cmdTrack.setOutputStream(baos);
        cmdTrack.run();
        return baos;
    }

    private String getLastLine(ByteArrayOutputStream baos) {
        String bla = new String(baos.toByteArray(), StandardCharsets.UTF_8);
        String[] lines = StringUtils.split(bla, "\n");
        return lines[lines.length - 1];
    }


}