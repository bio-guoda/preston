package bio.guoda.preston.cmd;

import bio.guoda.preston.store.KeyTo1LevelPath;
import net.lingala.zip4j.ZipFile;
import org.apache.commons.io.IOUtils;
import org.hamcrest.core.Is;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static junit.framework.TestCase.assertNotNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;

public class CmdCopyToTest {

    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    @Test
    public void generateJekyllSite() throws URISyntaxException, IOException {
        // https://github.com/bio-guoda/preston/issues/231
        URL resource = getClass().getResource("/bio/guoda/preston/cmd/bees-data.zip");
        assertNotNull(resource);

        ZipFile zipFile = new ZipFile(new File(resource.toURI()));
        File src1 = tmpDir.newFolder("src");
        zipFile.extractAll(src1.getAbsolutePath());
        File src = src1;
        File copyTo = tmpDir.newFolder("dst");
        CmdCopyTo cmdCopyTo = new CmdCopyTo();
        cmdCopyTo.setLocalDataDir(src.getAbsolutePath());
        cmdCopyTo.setTargetDir(copyTo.getAbsolutePath());
        cmdCopyTo.setArchiveType(ArchiveType.jekyll);

        assertThat(copyTo.list(), Is.is(new String[]{}));

        cmdCopyTo.run();

        assertNotNull(copyTo);
        assertNotNull(copyTo.list());
        List<String> actual = Arrays.asList(copyTo.list());
        assertThat(actual.size(), Is.is(9));
        assertThat(actual,
                (hasItems("assets","index.md","_includes","data.json",".gitignore","pages","_data", "_layouts", "registry.json")));

        File dataDir = new File(copyTo, "_data");

        File contentTable = new File(dataDir, "content.tsv");
        assertThat(contentTable.exists(), Is.is(true));

        String contentTableContent = IOUtils.toString(new FileInputStream(contentTable), StandardCharsets.UTF_8);

        assertThat(contentTableContent, Is.is("url\tverb\thash\tgraphname\ndonald\n"));


    }


    @Test
    public void trackAndCopy() throws URISyntaxException, IOException {
        // https://github.com/bio-guoda/preston/issues/166
        File src = prepareSrcDir();
        File copyTo = tmpDir.newFolder("dst");
        assertCopy(src, copyTo);
    }

    private File prepareSrcDir() throws URISyntaxException, IOException {
        URL resource = getClass().getResource("/bio/guoda/preston/cmd/copy-test-data.zip");
        assertNotNull(resource);

        ZipFile zipFile = new ZipFile(new File(resource.toURI()));
        File src = tmpDir.newFolder("src");
        zipFile.extractAll(src.getAbsolutePath());
        return src;
    }

    @Test
    public void trackAndCopyTwice() throws URISyntaxException, IOException {
        // https://github.com/bio-guoda/preston/issues/166
        File src = prepareSrcDir();

        File copyTo = tmpDir.newFolder("foo");
        assertCopy(src, copyTo);
        File copyToAgain = tmpDir.newFolder("bar");
        CmdCopyTo cmdCopyTo = new CmdCopyTo();
        cmdCopyTo.setLocalDataDir(copyTo.getAbsolutePath());
        cmdCopyTo.setTargetDir(copyToAgain.getAbsolutePath());
        cmdCopyTo.setKeyToPathLocal(new KeyTo1LevelPath(copyTo.toURI()));
        cmdCopyTo.setPathPattern(HashPathPattern.directoryDepth0);
        cmdCopyTo.setArchiveType(ArchiveType.data_prov_provindex);

        assertThat(copyToAgain.list(), Is.is(new String[]{}));

        cmdCopyTo.run();

        assertNotNull(copyToAgain);
        assertNotNull(copyToAgain.list());
        List<String> actual = Arrays.asList(copyToAgain.list());
        assertThat(actual.size(), Is.is(3));
        assertThat(actual,
                (hasItems("42df5238289c7a48655e3062dd6515b78a257ab0e3b93d37c2932b8a57c1ccc6", "ea405ba89c49c03628ef50145f326d22bca554b700244e4ba77d57a97e5b7c48", "2a5de79372318317a382ea9a2cef069780b852b01210ef59e06b640a3539cb5a")));
    }

    private void assertCopy(File sourceDir, File targetDir) throws IOException {
        CmdCopyTo cmdCopyTo = new CmdCopyTo();
        cmdCopyTo.setLocalDataDir(sourceDir.getAbsolutePath());
        cmdCopyTo.setTargetDir(targetDir.getAbsolutePath());
        cmdCopyTo.setPathPattern(HashPathPattern.directoryDepth0);
        cmdCopyTo.setArchiveType(ArchiveType.data_prov_provindex);

        assertThat(targetDir.list(), Is.is(new String[]{}));

        cmdCopyTo.run();

        assertNotNull(targetDir);
        assertNotNull(targetDir.list());
        List<String> actual = Arrays.asList(targetDir.list());
        assertThat(actual.size(), Is.is(3));
        assertThat(actual,
                (hasItems("42df5238289c7a48655e3062dd6515b78a257ab0e3b93d37c2932b8a57c1ccc6",
                        "ea405ba89c49c03628ef50145f326d22bca554b700244e4ba77d57a97e5b7c48",
                        "2a5de79372318317a382ea9a2cef069780b852b01210ef59e06b640a3539cb5a"))
        );
    }

}