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
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;

public class CmdHistoryTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void traceOrigin() throws IOException, URISyntaxException {
        CmdHistory cmd = new CmdHistory();

        URI indexAndDataRemote = getContentOnly();
        cmd.setRemotes(Collections.singletonList(indexAndDataRemote));
        cmd.setCacheEnabled(false);

        cmd.setLocalDataDir(folder.newFolder("data").getAbsolutePath());
        cmd.setProvenanceArchor(RefNodeFactory.toIRI("hash://sha256/87d3b8e67b0148f98abd0ff65be720993400ffc465764794a9afcdc00fe1a2e9"));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        cmd.setOutputStream(outputStream);

        cmd.run();

        assertThat(new String(outputStream.toByteArray(), StandardCharsets.UTF_8), Is.is(
                "<hash://sha256/87d3b8e67b0148f98abd0ff65be720993400ffc465764794a9afcdc00fe1a2e9> <http://www.w3.org/ns/prov#wasDerivedFrom> <hash://sha256/824d332100a58b29ee41c792725b115617b50821ec76aa8fcc058c2e8cf5413b> .\n" +
                "<hash://sha256/824d332100a58b29ee41c792725b115617b50821ec76aa8fcc058c2e8cf5413b> <http://www.w3.org/ns/prov#wasDerivedFrom> <hash://sha256/10b4f8ce7ed4faf6f3616cf12743ecc810bfe4db8cadf81f861d891b37c4ec29> .\n" +
                        "<urn:uuid:0659a54f-b713-4f86-a917-5be166a14110> <http://purl.org/pav/hasVersion> <hash://sha256/10b4f8ce7ed4faf6f3616cf12743ecc810bfe4db8cadf81f861d891b37c4ec29> .\n"));
    }

    @Test
    public void traceOriginUsingMD5Anchor() throws IOException, URISyntaxException {
        CmdHistory cmd = new CmdHistory();

        URI indexAndDataRemote = getContentOnly();
        cmd.setRemotes(Collections.singletonList(indexAndDataRemote));
        cmd.setCacheEnabled(false);

        cmd.setLocalDataDir(folder.newFolder("data").getAbsolutePath());
        cmd.setProvenanceArchor(RefNodeFactory.toIRI("hash://md5/b1a9328ef8106189402f566a6100e39d"));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        cmd.setOutputStream(outputStream);

        cmd.run();

        assertThat(new String(outputStream.toByteArray(), StandardCharsets.UTF_8), Is.is(
                "<hash://md5/b1a9328ef8106189402f566a6100e39d> <http://www.w3.org/ns/prov#wasDerivedFrom> <hash://sha256/824d332100a58b29ee41c792725b115617b50821ec76aa8fcc058c2e8cf5413b> .\n" +
                        "<hash://sha256/824d332100a58b29ee41c792725b115617b50821ec76aa8fcc058c2e8cf5413b> <http://www.w3.org/ns/prov#wasDerivedFrom> <hash://sha256/10b4f8ce7ed4faf6f3616cf12743ecc810bfe4db8cadf81f861d891b37c4ec29> .\n" +
                        "<urn:uuid:0659a54f-b713-4f86-a917-5be166a14110> <http://purl.org/pav/hasVersion> <hash://sha256/10b4f8ce7ed4faf6f3616cf12743ecc810bfe4db8cadf81f861d891b37c4ec29> .\n"));
    }


    @Test
    public void traceDescendants() throws IOException, URISyntaxException {
        CmdHistory cmd = new CmdHistory();
        cmd.setRemotes(Arrays.asList(getIndexAndContentRemote(), getContentOnly()));
        cmd.setLocalDataDir(folder.newFolder("data").getAbsolutePath());
        cmd.setCacheEnabled(false);


        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        cmd.setOutputStream(outputStream);

        cmd.run();

        assertThat(new String(outputStream.toByteArray(), StandardCharsets.UTF_8), Is.is(
                "<hash://sha256/688a0a1bebf9266b310df9c121fea4c6977b10cf473705af0bdd154f8bc0aa34> <http://www.w3.org/ns/prov#wasDerivedFrom> <hash://sha256/4a433c2a09146e4df5e860a26cf3ecd0d484ec372674b192b3963350e928d697> .\n" +
                        "<urn:uuid:0659a54f-b713-4f86-a917-5be166a14110> <http://purl.org/pav/hasVersion> <hash://sha256/4a433c2a09146e4df5e860a26cf3ecd0d484ec372674b192b3963350e928d697> .\n"));
    }

    private URI getIndexAndContentRemote() throws URISyntaxException {
        URL queryIndex = getClass().getResource("history/dataindex/2a/5d/2a5de79372318317a382ea9a2cef069780b852b01210ef59e06b640a3539cb5a");
        assertNotNull(queryIndex);

        File dataDir = new File(queryIndex.toURI()).getParentFile().getParentFile().getParentFile();

        assertThat(dataDir.getName(), Is.is("dataindex"));

        return toURI(dataDir);
    }

    private URI getContentOnly() throws URISyntaxException {
        URL queryIndex = getClass().getResource("history/datacontent/82/4d/824d332100a58b29ee41c792725b115617b50821ec76aa8fcc058c2e8cf5413b");
        assertNotNull(queryIndex);

        File dataDir = new File(queryIndex.toURI()).getParentFile().getParentFile().getParentFile();

        assertThat(dataDir.getName(), Is.is("datacontent"));

        return toURI(dataDir);
    }

    private URI toURI(File dataDir) throws URISyntaxException {
        return Paths.get(dataDir.toURI()).toUri();
    }


}