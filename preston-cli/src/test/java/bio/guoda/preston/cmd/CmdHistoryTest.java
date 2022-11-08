package bio.guoda.preston.cmd;

import bio.guoda.preston.RefNodeFactory;
import org.hamcrest.core.Is;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;

public class CmdHistoryTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void traceOrigin() throws IOException, URISyntaxException {
        CmdHistory cmd = new CmdHistory();

        URL queryIndex = getClass().getResource("history/datacontent/82/4d/824d332100a58b29ee41c792725b115617b50821ec76aa8fcc058c2e8cf5413b");
        assertNotNull(queryIndex);

        File dataDir = new File(queryIndex.toURI()).getParentFile().getParentFile().getParentFile();

        assertThat(dataDir.getName(), Is.is("datacontent"));

        cmd.setRemotes(Collections.singletonList(URI.create("file://" + dataDir.getAbsolutePath())));

        cmd.setLocalDataDir(folder.newFolder("data").getAbsolutePath());
        cmd.setProvenanceArchor(RefNodeFactory.toIRI("hash://sha256/824d332100a58b29ee41c792725b115617b50821ec76aa8fcc058c2e8cf5413b"));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        cmd.setOutputStream(outputStream);

        cmd.run();

        assertThat(new String(outputStream.toByteArray(), StandardCharsets.UTF_8), Is.is(
                "<hash://sha256/824d332100a58b29ee41c792725b115617b50821ec76aa8fcc058c2e8cf5413b> <http://www.w3.org/ns/prov#wasDerivedFrom> <hash://sha256/10b4f8ce7ed4faf6f3616cf12743ecc810bfe4db8cadf81f861d891b37c4ec29> .\n" +
                        "<urn:uuid:0659a54f-b713-4f86-a917-5be166a14110> <http://purl.org/pav/hasVersion> <hash://sha256/10b4f8ce7ed4faf6f3616cf12743ecc810bfe4db8cadf81f861d891b37c4ec29> .\n"));
    }

    @Test
    public void traceDescendants() throws IOException, URISyntaxException {
        CmdHistory cmdHistory = createCmdHistoryWithStaticRemote();
        cmdHistory.setLocalDataDir(folder.newFolder("data").getAbsolutePath());

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        cmdHistory.setOutputStream(outputStream);

        cmdHistory.run();

        assertThat(new String(outputStream.toByteArray(), StandardCharsets.UTF_8), Is.is(
                "<urn:uuid:0659a54f-b713-4f86-a917-5be166a14110> <http://purl.org/pav/hasVersion> <hash://sha256/c253a5311a20c2fc082bf9bac87a1ec5eb6e4e51ff936e7be20c29c8e77dee55> .\n" +
                "<hash://sha256/b83cf099449dae3f633af618b19d05013953e7a1d7d97bc5ac01afd7bd9abe5d> <http://purl.org/pav/previousVersion> <hash://sha256/c253a5311a20c2fc082bf9bac87a1ec5eb6e4e51ff936e7be20c29c8e77dee55> .\n" +
                "<hash://sha256/7efdea9263e57605d2d2d8b79ccd26a55743123d0c974140c72c8c1cfc679b93> <http://purl.org/pav/previousVersion> <hash://sha256/b83cf099449dae3f633af618b19d05013953e7a1d7d97bc5ac01afd7bd9abe5d> .\n" +
                "<hash://sha256/05a877bdb8617144fe166a13bf51828d4ad1bc11631c360b9e648a9f7df2bbcd> <http://purl.org/pav/previousVersion> <hash://sha256/7efdea9263e57605d2d2d8b79ccd26a55743123d0c974140c72c8c1cfc679b93> .\n" +
                "<hash://sha256/b5a30bbd8d51e9faf08d4ddebbc5bda9bab1b12545172f1524ac5ebdb0038bd4> <http://purl.org/pav/previousVersion> <hash://sha256/05a877bdb8617144fe166a13bf51828d4ad1bc11631c360b9e648a9f7df2bbcd> .\n"));
    }

    private CmdHistory createCmdHistoryWithStaticRemote() throws URISyntaxException {
        CmdHistory cmdHistory = new CmdHistory();

        URL queryIndex = getClass().getResource("history/dataindex/2a/5d/2a5de79372318317a382ea9a2cef069780b852b01210ef59e06b640a3539cb5a");
        assertNotNull(queryIndex);

        File dataDir = new File(queryIndex.toURI()).getParentFile().getParentFile().getParentFile();

        assertThat(dataDir.getName(), Is.is("dataindex"));

        cmdHistory.setRemotes(Collections.singletonList(URI.create("file://" + dataDir.getAbsolutePath())));
        return cmdHistory;
    }

}