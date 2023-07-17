package bio.guoda.preston.cmd;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.KeyTo1LevelOCIPath;
import org.hamcrest.core.Is;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

public class CmdGetIT {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void getDataOneSHA1() {
        CmdGet cmdGet = new CmdGet();
        cmdGet.setLocalDataDir(folder.getRoot().getAbsolutePath());
        cmdGet.setRemotes(Arrays.asList(URI.create("https://dataone.org")));
        cmdGet.setContentIdsOrAliases(Arrays.asList(RefNodeFactory.toIRI("hash://sha1/398ab74e3da160d52705bb2477eb0f2f2cde5f15")));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        cmdGet.setOutputStream(outputStream);
        cmdGet.run();

        assertThat(outputStream.size(), Is.is(3392786));
    }

    @Test
    public void getGitHubContentRepositoryAsRemove() {


        CmdGet cmdGet = new CmdGet();
        cmdGet.setLocalDataDir(folder.getRoot().getAbsolutePath());
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
        cmdGet.setLocalDataDir(folder.getRoot().getAbsolutePath());
        cmdGet.setRemotes(Arrays.asList(URI.create("https://dataone.org")));
        cmdGet.setContentIdsOrAliases(Arrays.asList(RefNodeFactory.toIRI("hash://sha256/bd2f8004d746be0b6e2abe08e7e21474bfd5ccd855734fe971a8631de1e2bf39")));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        cmdGet.setOutputStream(outputStream);
        cmdGet.run();

        assertThat(outputStream.size(), Is.is(90417));
    }

}