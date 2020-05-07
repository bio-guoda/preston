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

    public InputStream assertExpectedHash() throws IOException {
        String complete = "/bio/guoda/preston/process/idigbio-recordsets-complete.json";

        InputStream resourceAsStream = getClass().getResourceAsStream(complete);
        IRI hash = new HashGeneratorTLSHashIRI().hash(resourceAsStream);
        assertThat(hash.getIRIString(), is("hash://tlsh/1ac4d824c9a50ea305c621a9bdd94583e25052972e447c047f4c8b5c4feee2fbafa3dd"));
        return resourceAsStream;
    }

}