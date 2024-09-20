package bio.guoda.preston.cmd;

import bio.guoda.preston.RefNodeFactory;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.hamcrest.core.Is;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;

import static org.hamcrest.MatcherAssert.assertThat;

public class CmdHeadTest {

    @Rule
    public TemporaryFolder dataDir = new TemporaryFolder();


    @Test
    public void findHeadWithIndex() throws IOException {
        populateDataDir();
        CmdHead cmdHead = new CmdHead();

        cmdHead.setLocalDataDir(dataDir.getRoot().getAbsolutePath() + "/data");

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        cmdHead.setOutputStream(outputStream);

        cmdHead.run();

        String expectedHeadContentId = "hash://sha256/30845fefa4a854fc67da113a06759f86902b591bf0708bd625e611680aa1c9c4\n";
        assertThat(new String(outputStream.toByteArray(), StandardCharsets.UTF_8), Is.is(expectedHeadContentId));

    }

    private void populateDataDir() throws IOException {
        String file = getClass().getResource("data-with-foo-update.zip").getFile();
        try (ZipFile zipFile = ZipFile.builder().setFile(file).get()) {

            Enumeration<ZipArchiveEntry> entries = zipFile.getEntries();

            while (entries.hasMoreElements()) {
                ZipArchiveEntry zipArchiveEntry = entries.nextElement();
                File target = new File(dataDir.getRoot(), zipArchiveEntry.getName());
                if (zipArchiveEntry.isDirectory()) {
                    FileUtils.forceMkdir(target);
                } else {
                    IOUtils.copy(zipFile.getInputStream(zipArchiveEntry), new FileOutputStream(target));
                }
            }
        }
    }

    @Test
    public void findHeadWithAnchor() throws IOException {
        CmdHead cmdHead = new CmdHead();

        // note that data dir is *not* populated
        cmdHead.setLocalDataDir(dataDir.getRoot().getAbsolutePath() + "/data");
        cmdHead.setProvenanceArchor(RefNodeFactory.toIRI("hash://sha256/30845fefa4a854fc67da113a06759f86902b591bf0708bd625e611680aa1c9c4"));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        cmdHead.setOutputStream(outputStream);

        cmdHead.run();

        String expectedHeadContentId = "hash://sha256/30845fefa4a854fc67da113a06759f86902b591bf0708bd625e611680aa1c9c4\n";
        assertThat(new String(outputStream.toByteArray(), StandardCharsets.UTF_8), Is.is(expectedHeadContentId));

    }

    @Test
    public void findHeadWithAnchorAndUpdate() throws IOException {
        populateDataDir();
        CmdHead cmdHead = new CmdHead();

        // note that data dir *is* populated
        cmdHead.setLocalDataDir(dataDir.getRoot().getAbsolutePath() + "/data");
        // set anchor to first, but not most recent version
        cmdHead.setProvenanceArchor(RefNodeFactory.toIRI("hash://sha256/b1937f9fb1d84b02f2e0cd6e11018688fd009280394a7c1fd264c10de9b14998"));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        cmdHead.setOutputStream(outputStream);

        cmdHead.run();

        // expect most recent version
        String expectedHeadContentId = "hash://sha256/30845fefa4a854fc67da113a06759f86902b591bf0708bd625e611680aa1c9c4\n";
        assertThat(new String(outputStream.toByteArray(), StandardCharsets.UTF_8), Is.is(expectedHeadContentId));

    }

}