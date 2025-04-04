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
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

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


    @Test
    public void validCreateContentMakeAlias() throws URISyntaxException, IOException {
        CmdTrack cmd = new CmdTrack();
        cmd.setIRIs(Collections.singletonList(RefNodeFactory.toIRI(getClass().getResource("content.txt").toURI())));
        String dataDir = tmpFolder.newFolder("data").getAbsolutePath();
        cmd.setDataDir(dataDir);
        cmd.run();

        CmdAlias cmdAlias = new CmdAlias();
        IRI alias = RefNodeFactory.toIRI("my:content.txt");
        IRI contentId = RefNodeFactory.toIRI("hash://sha256/c7be1ed902fb8dd4d48997c6452f5d7e509fbcdbe2808b16bcf4edce4c07d14e");
        cmdAlias.setParams(Arrays.asList(alias, contentId));
        cmdAlias.setDataDir(dataDir);
        cmdAlias.run();

        CmdGet get = new CmdGet();
        get.setContentIdsOrAliases(Collections.singletonList(RefNodeFactory.toIRI("my:content.txt")));
        get.setDataDir(dataDir);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        get.setOutputStream(outputStream);

        get.run();

        assertThat(new String(outputStream.toByteArray(), StandardCharsets.UTF_8), Is.is("This is a test"));


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
        track.setOutputStream(NullOutputStream.INSTANCE);
        track.setDataDir(dataDir);
        track.setIRIs(Arrays.asList(RefNodeFactory.toIRI("https://example.org")));
        track.run();

        CmdAlias cmdAlias = new CmdAlias();
        cmdAlias.setDataDir(dataDir);
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
        track.setOutputStream(NullOutputStream.INSTANCE);
        track.setDataDir(dataDir);
        track.setIRIs(Arrays.asList(RefNodeFactory.toIRI("https://example.org")));
        track.run();

        CmdAlias cmdAlias = new CmdAlias();
        cmdAlias.setDataDir(dataDir);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        cmdAlias.setOutputStream(outputStream);
        cmdAlias.run();

        String output = new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
        String[] lines = StringUtils.split(output, "\n");
        assertThat(lines.length, Is.is(1));
        assertThat(lines[0], startsWith("<https://example.org> <http://purl.org/pav/hasVersion> <hash://sha256/"));
    }



}