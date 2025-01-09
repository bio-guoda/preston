package bio.guoda.preston.cmd;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.KeyTo1LevelPath;
import net.lingala.zip4j.ZipFile;
import org.apache.commons.io.IOUtils;
import org.hamcrest.core.Is;
import org.junit.Ignore;
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
        cmdCopyTo.setDataDir(src.getAbsolutePath());
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

        assertThat(contentTableContent, Is.is(
                "url\tverb\thash\tgraphname\n" +
                "https://search.idigbio.org/v2/search/records/?rq=%7B%22family%22%3A%22Andrenidae%22%2c%22hasImage%22%3A%22true%22%7D&limit=1&offset=0\thttp://purl.org/pav/hasVersion\thash://sha256/e094a0c1b7fbe977fc60d4e63bce17d2e93a2e9e30b82282760e95effa6ebc94\turn:uuid:f8672268-2dda-44a8-be5d-18889b1af614\n" +
                "https://search.idigbio.org/v2/view/mediarecords/9625f568-1001-4e35-97f3-6335c8526e0d\thttp://purl.org/pav/hasVersion\thash://sha256/2ff68ec8fb3c7e0ead608ab632619d960757593b50f8bc9eaaca17042ca93d4f\turn:uuid:62d2b280-8738-49bc-b8c5-80970425a318\n" +
                "https://api.idigbio.org/v2/media/9625f568-1001-4e35-97f3-6335c8526e0d?size=thumbnail\thttp://purl.org/pav/hasVersion\thash://sha256/92ec00809785532def79364e3d32cc593e6b3d3ee5875b6d5adf3adcad7049cc\turn:uuid:83d1af8a-6134-49ff-91d3-77cc7d7871c7\n" +
                "https://api.idigbio.org/v2/media/9625f568-1001-4e35-97f3-6335c8526e0d?size=webview\thttp://purl.org/pav/hasVersion\thash://sha256/92ec00809785532def79364e3d32cc593e6b3d3ee5875b6d5adf3adcad7049cc\turn:uuid:f107aae1-befe-4d9c-9508-418344e7ca5d\n" +
                "https://api.idigbio.org/v2/media/9625f568-1001-4e35-97f3-6335c8526e0d?size=fullsize\thttp://purl.org/pav/hasVersion\thash://sha256/92ec00809785532def79364e3d32cc593e6b3d3ee5875b6d5adf3adcad7049cc\turn:uuid:e12adadf-46d5-46f9-be4c-93a86315505c\n" +
                "https://iiif.mcz.harvard.edu/iiif/3/3812681/full/max/0/default.jpg\thttp://purl.org/pav/hasVersion\thash://sha256/d24fb91674817ea43a661045271e03f0278a4d7a78d32ecc8d2136a860ae1bf7\turn:uuid:bdda03fc-5d1b-49e4-aef9-33bdda4d6119\n"
        ));


    }


    @Test
    public void trackAndCopy() throws URISyntaxException, IOException {
        // https://github.com/bio-guoda/preston/issues/166
        File src = prepareSrcDir();
        File copyTo = tmpDir.newFolder("dst");
        assertCopy(src, copyTo);
    }

    @Test
    public void trackAndCopyTwoVersionsNoAnchhor() throws URISyntaxException, IOException {
        File src = prepareSrcDir(getClass().getResource("/bio/guoda/preston/cmd/data-two-versions.zip"));
        File copyTo = tmpDir.newFolder("dst");
        CmdCopyTo cmdCopyTo = new CmdCopyTo();
        cmdCopyTo.setDataDir(src.getAbsolutePath());
        cmdCopyTo.setTargetDir(copyTo.getAbsolutePath());
        cmdCopyTo.setPathPattern(HashPathPattern.directoryDepth0);
        cmdCopyTo.setArchiveType(ArchiveType.data_prov_provindex);

        assertThat(copyTo.list(), Is.is(new String[]{}));

        cmdCopyTo.run();

        assertNotNull(copyTo);
        assertNotNull(copyTo.list());
        List<String> actual = Arrays.asList(copyTo.list());
        assertThat(actual.size(), Is.is(5));
        assertThat(actual,
                (hasItems("71d606eab8fd7e0d8ac4c263ca725d4a88f0d0199602e79f686fd03f260eebf7",
                        "ad5b025a1f9eee92412429dd9d42297703c13367ac0eb6fbe33092de90988bdc",
                        "df1d4e1f20e1d8f89ddbb0fccb9f8005283c334a6e5a6fb19e30720bed2a9c1c",
                        "ea8fac7c65fb589b0d53560f5251f74f9e9b243478dcb6b3ea79b5e36449c8d9",
                        "2a5de79372318317a382ea9a2cef069780b852b01210ef59e06b640a3539cb5a"))
        );
    }


    @Ignore("https://github.com/bio-guoda/preston/issues/274")
    @Test
    public void trackAndCopyTwoVersionsSpecificAnchhor() throws URISyntaxException, IOException {
        File src = prepareSrcDir(getClass().getResource("/bio/guoda/preston/cmd/data-two-versions.zip"));
        File copyTo = tmpDir.newFolder("dst");
        CmdCopyTo cmdCopyTo = new CmdCopyTo();
        cmdCopyTo.setDataDir(src.getAbsolutePath());
        cmdCopyTo.setTargetDir(copyTo.getAbsolutePath());
        cmdCopyTo.setProvenanceArchor(RefNodeFactory.toIRI("hash://sha256/df1d4e1f20e1d8f89ddbb0fccb9f8005283c334a6e5a6fb19e30720bed2a9c1c"));
        cmdCopyTo.setPathPattern(HashPathPattern.directoryDepth0);
        cmdCopyTo.setArchiveType(ArchiveType.data_prov_provindex);

        assertThat(copyTo.list(), Is.is(new String[]{}));

        cmdCopyTo.run();

        assertNotNull(copyTo);
        assertNotNull(copyTo.list());
        List<String> actual = Arrays.asList(copyTo.list());
        assertThat(actual.size(), Is.is(3));
        assertThat(actual,
                (hasItems("df1d4e1f20e1d8f89ddbb0fccb9f8005283c334a6e5a6fb19e30720bed2a9c1c",
                        "ea8fac7c65fb589b0d53560f5251f74f9e9b243478dcb6b3ea79b5e36449c8d9",
                        "2a5de79372318317a382ea9a2cef069780b852b01210ef59e06b640a3539cb5a"))
        );
    }

    private File prepareSrcDir() throws URISyntaxException, IOException {
        URL resource = getClass().getResource("/bio/guoda/preston/cmd/copy-test-data.zip");
        assertNotNull(resource);

        return prepareSrcDir(resource);
    }

    private File prepareSrcDir(URL resource) throws URISyntaxException, IOException {
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
        cmdCopyTo.setDataDir(copyTo.getAbsolutePath());
        cmdCopyTo.setTargetDir(copyToAgain.getAbsolutePath());
        cmdCopyTo.setDepth(0);
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
        cmdCopyTo.setDataDir(sourceDir.getAbsolutePath());
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