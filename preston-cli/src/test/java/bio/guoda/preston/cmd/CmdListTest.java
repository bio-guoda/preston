package bio.guoda.preston.cmd;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.TestUtil;
import org.apache.commons.io.IOUtils;
import org.hamcrest.core.Is;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertNotNull;

public class CmdListTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void listOrigins() throws URISyntaxException, IOException {
        CmdList cmd = new CmdList();

        String provLog1 = "history/datacontent/82/4d/824d332100a58b29ee41c792725b115617b50821ec76aa8fcc058c2e8cf5413b";
        String provLog2 = "history/datacontent/10/b4/10b4f8ce7ed4faf6f3616cf12743ecc810bfe4db8cadf81f861d891b37c4ec29";
        URL queryIndex = getClass().getResource(provLog1);
        assertNotNull(queryIndex);

        File dataDir = new File(queryIndex.toURI()).getParentFile().getParentFile().getParentFile();

        assertThat(dataDir.getName(), Is.is("datacontent"));


        cmd.setRemotes(Collections.singletonList(Paths.get(dataDir.toURI()).toUri()));

        cmd.setDataDir(folder.newFolder("data").getAbsolutePath());
        cmd.setProvenanceArchor(RefNodeFactory.toIRI("hash://sha256/824d332100a58b29ee41c792725b115617b50821ec76aa8fcc058c2e8cf5413b"));
        cmd.setCacheEnabled(false);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        cmd.setOutputStream(outputStream);

        cmd.run();

        ByteArrayOutputStream expected = new ByteArrayOutputStream();

        IOUtils.copy(getClass().getResourceAsStream(provLog1), expected);
        IOUtils.copy(getClass().getResourceAsStream(provLog2), expected);


        String actualString = new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
        String expectedString = new String(expected.toByteArray(), StandardCharsets.UTF_8);

        assertThat(TestUtil.removeCarriageReturn(actualString), Is.is(TestUtil.removeCarriageReturn(expectedString)));

    }

    @Test
    public void listOriginsAsTSV() throws URISyntaxException, IOException {
        CmdList cmd = new CmdList();

        String tsvProvLog1 = "history/datacontent/82/4d/824d332100a58b29ee41c792725b115617b50821ec76aa8fcc058c2e8cf5413b.tsv";
        String tsvProvLog2 = "history/datacontent/10/b4/10b4f8ce7ed4faf6f3616cf12743ecc810bfe4db8cadf81f861d891b37c4ec29.tsv";
        URL queryIndex = getClass().getResource(tsvProvLog1);
        assertNotNull(queryIndex);

        File dataDir = new File(queryIndex.toURI()).getParentFile().getParentFile().getParentFile();

        assertThat(dataDir.getName(), Is.is("datacontent"));


        cmd.setRemotes(Collections.singletonList(Paths.get(dataDir.toURI()).toUri()));

        cmd.setDataDir(folder.newFolder("data").getAbsolutePath());
        cmd.setProvenanceArchor(RefNodeFactory.toIRI("hash://sha256/824d332100a58b29ee41c792725b115617b50821ec76aa8fcc058c2e8cf5413b"));
        cmd.setCacheEnabled(false);
        cmd.setLogMode(LogTypes.tsv);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        cmd.setOutputStream(outputStream);

        cmd.run();

        ByteArrayOutputStream expected = new ByteArrayOutputStream();

        IOUtils.copy(getClass().getResourceAsStream(tsvProvLog1), expected);
        IOUtils.copy(getClass().getResourceAsStream(tsvProvLog2), expected);

        String actualTSVString = new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
        String expectedTSVString = new String(expected.toByteArray(), StandardCharsets.UTF_8);

        String actual = TestUtil.removeCarriageReturn(actualTSVString);
        IOUtils.copy(IOUtils.toInputStream(actual, StandardCharsets.UTF_8), new FileOutputStream("target/origins.tsv"));
        assertThat(actual, Is.is(TestUtil.removeCarriageReturn(expectedTSVString)));

    }

}