package bio.guoda.preston;

import org.apache.commons.rdf.api.IRI;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class HashGeneratorSHA256Test {

    @Test
    public void hashText() throws IOException {
        IRI hash = new HashGeneratorSHA256().hash(getClass().getResourceAsStream("/bio/guoda/preston/process/idigbio-recordsets-complete.json"));
        assertThat(hash.getIRIString(), is("hash://sha256/0931a831be557bfedd27b0068d9a0a9f14c1b92cbf9199e8ba79e04d0a6baedc"));
    };

    @Test
    public void hashBinary() throws IOException {
        IRI hash = new HashGeneratorSHA256().hash(getClass().getResourceAsStream("/bio/guoda/preston/dwca-20180905.zip"));
        assertThat(hash.getIRIString(), is("hash://sha256/59f32445a50646d923f8ba462a7d87a848632f28bd93ac579de210e3375714de"));
    };

}