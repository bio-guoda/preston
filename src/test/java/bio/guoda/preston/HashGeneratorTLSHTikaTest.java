package bio.guoda.preston;

import org.apache.commons.rdf.api.IRI;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class HashGeneratorTLSHTikaTest {

    private static final String DWCA = "/bio/guoda/preston/dwca-20180905.zip";
    private static final String DWCA_REPACKAGED = "/bio/guoda/preston/dwca-20180905-repackaged.zip";

    @Test
    public void calculateLTSHash() throws IOException {
        IRI hash = new HashGeneratorTLSHTika().hash(getClass().getResourceAsStream(DWCA));
        assertThat(hash.getIRIString(), is("hash://tika/532a4237d1782aa7576f40d213f91ce46b1fb886498bebcedc507680db323a9415f"));
    }

    @Test
    public void calculateLTSHashDifferentRepackagedBinary() throws IOException {
        IRI hash = new HashGeneratorTLSHTika().hash(getClass().getResourceAsStream(DWCA));
        IRI hashRepackaged = new HashGeneratorTLSHTika().hash(getClass().getResourceAsStream(DWCA_REPACKAGED));
        assertThat(hash.getIRIString(), is(hashRepackaged.getIRIString()));
    }


    @Test(expected = IOException.class)
    public void inputStreamClosed() throws IOException {
        InputStream resourceAsStream1 = getClass().getResourceAsStream(DWCA);
        IRI hash = new HashGeneratorTLSHTika().hash(resourceAsStream1);
        assertThat(hash.getIRIString(), is("hash://tika/532a4237d1782aa7576f40d213f91ce46b1fb886498bebcedc507680db323a9415f"));
        resourceAsStream1.read();
    }

}