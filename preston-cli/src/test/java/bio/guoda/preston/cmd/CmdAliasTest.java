package bio.guoda.preston.cmd;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.process.StatementsListenerAdapter;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.hamcrest.core.Is;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static junit.framework.TestCase.fail;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;

public class CmdAliasTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();


    @Test(expected = IllegalArgumentException.class)
    public void invalidAlias() {
        CmdAlias cmdAlias = new CmdAlias();
        IRI alias = RefNodeFactory.toIRI("hash://sha256/e0c131ebf6ad2dce71ab9a10aa116dcedb219ae4539f9e5bf0e57b84f51f22ca");
        IRI contentId = RefNodeFactory.toIRI("hash://sha256/f0c131ebf6ad2dce71ab9a10aa116dcedb219ae4539f9e5bf0e57b84f51f22ca");
        cmdAlias.setParams(Arrays.asList(alias, contentId));
        cmdAlias.run(new StatementsListenerAdapter() {
            @Override
            public void on(Quad statement) {
                fail("should not work");
            }
        });
    }

    @Test
    public void validAlias() {
        CmdAlias cmdAlias = new CmdAlias();
        IRI alias = RefNodeFactory.toIRI("myfinalpaper.txt");
        IRI contentId = RefNodeFactory.toIRI("hash://sha256/f0c131ebf6ad2dce71ab9a10aa116dcedb219ae4539f9e5bf0e57b84f51f22ca");
        cmdAlias.setParams(Arrays.asList(alias, contentId));
        cmdAlias.run(new StatementsListenerAdapter() {
            @Override
            public void on(Quad statement) {
                fail("should not work");
            }
        });
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidAliasTarget() {
        CmdAlias cmdAlias = new CmdAlias();
        IRI alias = RefNodeFactory.toIRI("myfinalpaper.txt");
        IRI contentId = RefNodeFactory.toIRI("foo:bar:123");
        cmdAlias.setParams(Arrays.asList(alias, contentId));
        cmdAlias.run(new StatementsListenerAdapter() {
            @Override
            public void on(Quad statement) {
                fail("should not work");
            }
        });
    }

    @Test
    public void showTrackedAlias() {
        String dataDir = tmpFolder.getRoot().getAbsolutePath();
        CmdTrack track = new CmdTrack();
        track.setOutputStream(NullOutputStream.NULL_OUTPUT_STREAM);
        track.setLocalDataDir(dataDir);
        track.setIRIs(Arrays.asList(RefNodeFactory.toIRI("https://example.org")));
        track.run();

        CmdAlias cmdAlias = new CmdAlias();
        cmdAlias.setLocalDataDir(dataDir);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        cmdAlias.setOutputStream(outputStream);
        cmdAlias.run();

        String output = new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
        String[] lines = StringUtils.split(output, "\n");
        assertThat(lines.length, Is.is(1));
        assertThat(lines[0], startsWith("<https://example.org> <http://purl.org/pav/hasVersion> <hash://sha256/"));
    }

    @Test
    public void showUUIDAlias() {
        String dataDir = tmpFolder.getRoot().getAbsolutePath();
        CmdTrack track = new CmdTrack();
        track.setOutputStream(NullOutputStream.NULL_OUTPUT_STREAM);
        track.setLocalDataDir(dataDir);
        track.setIRIs(Arrays.asList(RefNodeFactory.toIRI("https://example.org")));
        track.run();

        CmdAlias cmdAlias = new CmdAlias();
        cmdAlias.setLocalDataDir(dataDir);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        cmdAlias.setOutputStream(outputStream);
        cmdAlias.run();

        String output = new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
        String[] lines = StringUtils.split(output, "\n");
        assertThat(lines.length, Is.is(1));
        assertThat(lines[0], startsWith("<https://example.org> <http://purl.org/pav/hasVersion> <hash://sha256/"));
    }



}