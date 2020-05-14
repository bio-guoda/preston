package bio.guoda.preston.cmd;

import bio.guoda.preston.HashType;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

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

    @Test(expected = RuntimeException.class)
    public void calcLTSHTooShort() {
        CmdHash cmdHash = new CmdHash();
        cmdHash.setInputStream(IOUtils.toInputStream("hello", StandardCharsets.UTF_8));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        cmdHash.setOutputStream(out);
        cmdHash.setHashAlgorithm(HashType.tlsh);
        cmdHash.run();
    }

    @Test
    public void calcLTSH() {
        CmdHash cmdHash = new CmdHash();
        cmdHash.setInputStream(getClass().getResourceAsStream("/bio/guoda/preston/dwca-20180905.zip"));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        cmdHash.setOutputStream(out);
        cmdHash.setHashAlgorithm(HashType.tlsh);
        cmdHash.run();

        assertThat(new String(out.toByteArray(), StandardCharsets.UTF_8),
                is("hash://tlsh/423681883b2b6f3366674b3914a9142a378ad1f41f6641bc84cfa30bb715d7dedc8\n"));
    }

    @Test
    public void calcLTSHTika() {
        CmdHash cmdHash = new CmdHash();
        cmdHash.setInputStream(getClass().getResourceAsStream("/bio/guoda/preston/dwca-20180905.zip"));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        cmdHash.setOutputStream(out);
        cmdHash.setHashAlgorithm(HashType.tika_tlsh);
        cmdHash.run();

        assertThat(new String(out.toByteArray(), StandardCharsets.UTF_8),
                is("hash://tika-tlsh/532a4237d1782aa7576f40d213f91ce46b1fb886498bebcedc507680db323a9415f\n"));
    }

}