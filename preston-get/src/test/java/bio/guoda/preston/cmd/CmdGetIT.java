package bio.guoda.preston.cmd;

import bio.guoda.preston.HashType;
import bio.guoda.preston.RefNodeFactory;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.hamcrest.core.Is;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

public class CmdGetIT {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void getDataOneSHA1() {
        CmdGet cmdGet = new CmdGet();
        cmdGet.setDataDir(folder.getRoot().getAbsolutePath());
        cmdGet.setRemotes(Arrays.asList(URI.create("https://dataone.org")));
        cmdGet.setContentIdsOrAliases(Arrays.asList(RefNodeFactory.toIRI("hash://sha1/398ab74e3da160d52705bb2477eb0f2f2cde5f15")));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        cmdGet.setOutputStream(outputStream);
        cmdGet.run();

        assertThat(outputStream.size(), Is.is(3392786));
    }


    @Test
    public void getZenodoRestricted() {
        CmdGet cmdGet = new CmdGet();
        cmdGet.setDataDir(folder.getRoot().getAbsolutePath());
        cmdGet.setRemotes(Arrays.asList(URI.create("https://zenodo.org")));
        cmdGet.setContentIdsOrAliases(Arrays.asList(RefNodeFactory.toIRI("hash://md5/587f269cfa00aa40b7b50243ea8bdab9")));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        cmdGet.setOutputStream(outputStream);
        cmdGet.run();

        assertThat(outputStream.size(), Is.is(3392786));
    }

    @Test
    public void getZenodoOpen() {
        CmdGet cmdGet = new CmdGet();
        cmdGet.setDataDir(folder.getRoot().getAbsolutePath());
        cmdGet.setRemotes(Arrays.asList(URI.create("https://zenodo.org")));
        cmdGet.setContentIdsOrAliases(Arrays.asList(RefNodeFactory.toIRI("hash://md5/e124dfa939ce3adfd06401b43c216fed")));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        cmdGet.setOutputStream(outputStream);
        cmdGet.run();

        assertThat(outputStream.size(), Is.is(1148893));
    }

    @Test
    public void getZenodoInDataZipByAnchor() {
        // see also
        // https://github.com/bio-guoda/preston/issues/356
        // with example content associated with
        // Elton, Nomer, & Preston. (2025). Versioned Archive and Review of Biotic Interactions and Taxon Names Found within globalbioticinteractions/vertnet hash://md5/6c194df8ddb37844f7b4f1258c81d93d. Zenodo. https://doi.org/10.5281/zenodo.16915755
        CmdGet cmdGet = new CmdGet();
        cmdGet.setDataDir(folder.getRoot().getAbsolutePath());
        cmdGet.setHashType(HashType.md5);
        cmdGet.setRemotes(Arrays.asList(URI.create("https://zenodo.org")));
        cmdGet.setProvenanceArchor(RefNodeFactory.toIRI("hash://md5/838ede23f4b8c6e8b7d692b61e954a60"));
        cmdGet.setContentIdsOrAliases(Arrays.asList(RefNodeFactory.toIRI("hash://md5/2c6cbababdd943985e3ae4e064bb5b37")));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        cmdGet.setOutputStream(outputStream);
        cmdGet.run();

        assertEdgeList(outputStream);
    }

    @Test
    public void getZenodoInDataZip() {
        // see also
        // Elton, Nomer, & Preston. (2025). Versioned Archive and Review of Biotic Interactions and Taxon Names Found within globalbioticinteractions/vertnet hash://md5/6c194df8ddb37844f7b4f1258c81d93d. Zenodo. https://doi.org/10.5281/zenodo.16915755
        CmdGet cmdGet = new CmdGet();
        cmdGet.setDataDir(folder.getRoot().getAbsolutePath());
        cmdGet.setHashType(HashType.md5);
        cmdGet.setRemotes(Arrays.asList(URI.create("https://zenodo.org")));
        cmdGet.setProvenanceArchor(RefNodeFactory.toIRI("hash://md5/838ede23f4b8c6e8b7d692b61e954a60"));
        cmdGet.setContentIdsOrAliases(Arrays.asList(RefNodeFactory.toIRI("zip:hash://md5/c0307712e8ed5afc64c7dbdfc0c04e4b!/data/2c/6c/2c6cbababdd943985e3ae4e064bb5b37")));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        cmdGet.setOutputStream(outputStream);
        cmdGet.run();

        assertEdgeList(outputStream);
    }

    private static void assertEdgeList(ByteArrayOutputStream outputStream) {
        assertThat(outputStream.toByteArray().length, is(greaterThan(0)));

        String content = new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
        String[] lines = StringUtils.split(content, "\n");

        assertThat(StringUtils.trim(lines[0]), Is.is("Source,Target,Type,Id,Label,Weight"));
        assertThat(StringUtils.trim(lines[1]), Is.is("Canis latrans,Lepus californicus,Directed,0,,1"));
    }

    @Test
    public void getDataOneSHA1TwoBytes() {
        CmdGet cmdGet = new CmdGet();
        cmdGet.setDataDir(folder.getRoot().getAbsolutePath());
        cmdGet.setRemotes(Arrays.asList(URI.create("https://dataone.org")));
        cmdGet.setContentIdsOrAliases(Arrays.asList(RefNodeFactory.toIRI("cut:hash://sha1/398ab74e3da160d52705bb2477eb0f2f2cde5f15!/b1-2")));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        cmdGet.setOutputStream(outputStream);
        cmdGet.run();

        assertThat(outputStream.size(), Is.is(2));
        assertThat(new String(outputStream.toByteArray(), StandardCharsets.UTF_8), Is.is("bla"));
    }

    @Test
    public void getDataOneSHA1TwoBytesStdin() {
        CmdGet cmdGet = new CmdGet();
        cmdGet.setDataDir(folder.getRoot().getAbsolutePath());
        cmdGet.setRemotes(Arrays.asList(URI.create("https://dataone.org")));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        cmdGet.setInputStream(IOUtils.toInputStream("<https://example.org> <http://purl.org/pav/hasVersion> <cut:hash://sha1/398ab74e3da160d52705bb2477eb0f2f2cde5f15!/b1-2> .", StandardCharsets.UTF_8));
        cmdGet.setOutputStream(outputStream);
        cmdGet.run();

        assertThat(outputStream.size(), Is.is(2));
        assertThat(new String(outputStream.toByteArray(), StandardCharsets.UTF_8), Is.is("bla"));
    }

    @Test
    public void getGitHubContentRepositoryAsRemote() {
        CmdGet cmdGet = new CmdGet();
        cmdGet.setDataDir(folder.getRoot().getAbsolutePath());
        cmdGet.setRemotes(Arrays.asList(URI.create("https://ghcr.io/cboettig/content-store")));
        cmdGet.setContentIdsOrAliases(Arrays.asList(RefNodeFactory.toIRI("hash://sha256/9412325831dab22aeebdd674b6eb53ba6b7bdd04bb99a4dbb21ddff646287e37")));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        cmdGet.setOutputStream(outputStream);
        cmdGet.run();

        assertThat(outputStream.size(), Is.is(11036));
    }

    @Test
    public void getDataOneSHA256() {
        CmdGet cmdGet = new CmdGet();
        cmdGet.setDataDir(folder.getRoot().getAbsolutePath());
        cmdGet.setRemotes(Arrays.asList(URI.create("https://dataone.org")));
        cmdGet.setContentIdsOrAliases(Arrays.asList(RefNodeFactory.toIRI("hash://sha256/bd2f8004d746be0b6e2abe08e7e21474bfd5ccd855734fe971a8631de1e2bf39")));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        cmdGet.setOutputStream(outputStream);
        cmdGet.run();

        assertThat(outputStream.size(), Is.is(90417));
    }

}