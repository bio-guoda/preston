package bio.guoda.preston;

import org.apache.commons.rdf.api.IRI;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class HashGeneratorTLSHashIRITest {

    @Test
    public void calculateLTSHash() throws IOException {
        assertExpectedHash();
    }

    @Test(expected = IOException.class)
    public void inputStreamClosed() throws IOException {
        InputStream resourceAsStream = assertExpectedHash();
        resourceAsStream.read();
    }

    private InputStream assertExpectedHash() throws IOException {
        String complete = "/bio/guoda/preston/dwca-20180905.zip";

        InputStream resourceAsStream = getClass().getResourceAsStream(complete);
        IRI hash = new HashGeneratorTLSHashIRI().hash(resourceAsStream);
        assertThat(hash.getIRIString(), is("hash://tlsh/2d6423681883b2b6f3366674b3914a9142a378ad1f41f6641bc84cfa30bb715d7dedc8"));
        return resourceAsStream;
    }

}