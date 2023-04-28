package bio.guoda.preston.cmd;

import bio.guoda.preston.RDFUtil;
import bio.guoda.preston.RefNodeFactory;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDFTerm;
import org.hamcrest.core.Is;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertNotNull;

public class CmdTrackTest {

    @Rule
    public TemporaryFolder dataDir = new TemporaryFolder();


    @Ignore("https://github.com/bio-guoda/preston/issues/155")
    @Test
    public void trackWithoutCache() throws IOException {
        assertNumberOfDataFiles(2, false);

    }

    @Test
    public void trackWithCache() throws IOException {
        assertNumberOfDataFiles(3, true);

    }

    private void assertNumberOfDataFiles(int expectedNumberOfFiles, boolean enableCache) {
        CmdTrack cmd = new CmdTrack();

        String localDataDir = dataDir.getRoot().getAbsolutePath() + "/data";
        cmd.setLocalDataDir(localDataDir);
        cmd.setIRIs(Collections.singletonList(RefNodeFactory.toIRI("https://example.org")));
        cmd.setCacheEnabled(enableCache);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        cmd.setOutputStream(outputStream);


        assertThat(new File(localDataDir).exists(), Is.is(false));

        cmd.run();

        String actual = new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
        List<Quad> quads = RDFUtil.parseQuads(IOUtils.toInputStream(actual, StandardCharsets.UTF_8));

        Quad quad = quads.get(quads.size() - 1);
        RDFTerm object = quad.getObject();
        String contentId = RDFUtil.getValueFor(object);
        assertThat(contentId, startsWith("hash://sha256/"));

        File file = new File(localDataDir);
        assertNotNull(file);
        assertThat(file.list().length, Is.is(expectedNumberOfFiles));
    }


}