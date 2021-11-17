package bio.guoda.preston.cmd;

import bio.guoda.preston.HashType;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class CmdHashTest {

    @Test
    public void calcHash() {
        CmdHash cmdHash = new CmdHash();
        cmdHash.setInputStream(IOUtils.toInputStream("hello", StandardCharsets.UTF_8));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        cmdHash.setOutputStream(out);
        cmdHash.run();

        assertThat(new String(out.toByteArray(), StandardCharsets.UTF_8),
                is("hash://sha256/2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824\n"));
    }

}