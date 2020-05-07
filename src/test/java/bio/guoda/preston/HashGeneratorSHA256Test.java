package bio.guoda.preston;

import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class HashGeneratorSHA256Test {

    @Test
    public void hashTextFromResource() throws IOException {
        InputStream is = getClass().getResourceAsStream("/bio/guoda/preston/process/idigbio-recordsets-complete.json");
        IRI hash = new HashGeneratorSHA256().hash(is);
        assertThat(hash.getIRIString(), is("hash://sha256/0931a831be557bfedd27b0068d9a0a9f14c1b92cbf9199e8ba79e04d0a6baedc"));
    };

    @Test
    public void hashTextFromString() throws IOException {
        IRI hash = new HashGeneratorSHA256().hash(IOUtils.toInputStream("hello", StandardCharsets.UTF_8));
        assertThat(hash.getIRIString(), is("hash://sha256/2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"));
    };

    @Test
    public void hashBinary() throws IOException {
        IRI hash = new HashGeneratorSHA256().hash(getClass().getResourceAsStream("/bio/guoda/preston/dwca-20180905.zip"));
        assertThat(hash.getIRIString(), is("hash://sha256/59f32445a50646d923f8ba462a7d87a848632f28bd93ac579de210e3375714de"));
    };

}