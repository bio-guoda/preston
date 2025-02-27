package bio.guoda.preston.cmd;

import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

public class CmdVerifyTest {

    @Test
    public void verifySha256() throws URISyntaxException {

        ByteArrayOutputStream stdout = configAndRun("/bio/guoda/preston/cmd/verify-data/2a/5d/2a5de79372318317a382ea9a2cef069780b852b01210ef59e06b640a3539cb5a");

        String actual = new String(stdout.toByteArray(), StandardCharsets.UTF_8);
        assertThat(actual, Is.is(not("")));
        assertThat(actual, containsString("f0c131ebf6ad2dce71ab9a10aa116dcedb219ae4539f9e5bf0e57b84f51f22ca\tFAIL\tMISSING\t"));
    }

    @Test
    public void verifySha256InnerHash() throws URISyntaxException {

        ByteArrayOutputStream stdout = configAndRun("/bio/guoda/preston/cmd/verify-data/2a/5d/2a5de79372318317a382ea9a2cef069780b852b01210ef59e06b640a3539cb5a");

        String actual = new String(stdout.toByteArray(), StandardCharsets.UTF_8);
        assertThat(actual, Is.is(not("")));
        assertThat(actual, containsString("f0c131ebf6ad2dce71ab9a10aa116dcedb219ae4539f9e5bf0e57b84f51f22ca\tFAIL\tMISSING\t"));
        assertThat(actual, not(containsString("line:hash://sha256/854831f2665681track818b902104aa1212a1f1e38a2307a7d66ca1dfad4d8240c!/L1")));
    }

    private ByteArrayOutputStream configAndRun(String resourcePath) throws URISyntaxException {
        URL resource = getClass().getResource(resourcePath);

        File dataDir = new File(resource.toURI()).getParentFile().getParentFile().getParentFile();

        CmdVerify cmdVerify = new CmdVerify();
        ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        cmdVerify.setOutputStream(stdout);
        cmdVerify.setDataDir(dataDir.getAbsolutePath());
        cmdVerify.run();
        return stdout;
    }

}