package bio.guoda.preston.cmd;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNotNull;

public class CmdRISStreamTest  {

    @Test
    public void sciELORedirectDOI() throws URISyntaxException, IOException {
        URL resource = getClass().getResource("/bio/guoda/preston/cmd/ris-scielo-data/2a/5d/2a5de79372318317a382ea9a2cef069780b852b01210ef59e06b640a3539cb5a");
        assertNotNull(resource);
        File dataDir = new File(resource.toURI()).getParentFile().getParentFile().getParentFile();
        String absolutePath = dataDir.getAbsolutePath();

        CmdRISStream cmd = new CmdRISStream();
        cmd.setDataDir(absolutePath);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        cmd.setInputStream(getClass().getResourceAsStream("/bio/guoda/preston/cmd/ris-scielo-data/83/a7/83a7bcb1e340755edf2af171b13fe5628b81f113ff5062c90a527e1cff9558c4"));
        cmd.setOutputStream(outputStream);
        cmd.run();

        String actualMetadata = new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
        String actualMetadataFormatted
                = new ObjectMapper().readTree(actualMetadata).toPrettyString();

        InputStream resourceAsStream = getClass().getResourceAsStream("ris-scielo-data/zenodo-meta-expected.json");
        assertNotNull(resourceAsStream);
        String expectedMetadata = IOUtils.toString(resourceAsStream, StandardCharsets.UTF_8);
        assertThat(actualMetadataFormatted, is(expectedMetadata));

    }

}