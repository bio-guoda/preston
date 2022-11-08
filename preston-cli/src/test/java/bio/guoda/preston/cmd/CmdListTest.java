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

public class CmdListTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void listOrigins() throws URISyntaxException, IOException {
        CmdList cmd = new CmdList();

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

}