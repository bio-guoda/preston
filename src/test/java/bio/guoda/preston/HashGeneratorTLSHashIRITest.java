package bio.guoda.preston;

import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.Is.is;

public class HashGeneratorTLSHashIRITest {

    private static final String DWCA = "/bio/guoda/preston/dwca-20180905.zip";
    private static final String DWCA_REPACKAGED = "/bio/guoda/preston/dwca-20180905-repackaged.zip";

    @Test(expected = IOException.class)
    public void calculateLTSHashTooShort() throws IOException {
        new HashGeneratorTLSHashIRI().hash(IOUtils.toInputStream("hello", StandardCharsets.UTF_8));
    }

    @Test
    public void calculateLTSHash() throws IOException {
        IRI hash = new HashGeneratorTLSHashIRI().hash(getClass().getResourceAsStream(DWCA));
        assertThat(hash.getIRIString(), is("hash://tlsh/423681883b2b6f3366674b3914a9142a378ad1f41f6641bc84cfa30bb715d7dedc8"));
    }

    @Test
    public void calculateLTSHashDifferentRepackagedBinary() throws IOException {
        IRI hash = new HashGeneratorTLSHashIRI().hash(getClass().getResourceAsStream(DWCA));
        IRI hashRepackaged = new HashGeneratorTLSHashIRI().hash(getClass().getResourceAsStream(DWCA_REPACKAGED));
        assertThat(hash.getIRIString(), is(not(hashRepackaged.getIRIString())));
    }


    @Test(expected = IOException.class)
    public void inputStreamClosed() throws IOException {
        InputStream resourceAsStream1 = getClass().getResourceAsStream(DWCA);
        IRI hash = new HashGeneratorTLSHashIRI().hash(resourceAsStream1);
        assertThat(hash.getIRIString(), is("hash://tlsh/423681883b2b6f3366674b3914a9142a378ad1f41f6641bc84cfa30bb715d7dedc8"));
        resourceAsStream1.read();
    }

}