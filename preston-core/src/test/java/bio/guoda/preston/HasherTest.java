package bio.guoda.preston;

import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class HasherTest {

    @Test
    public void testSHA256() throws IOException {
        assertSHA(Hasher.calcSHA256(
                IOUtils.toInputStream("something", StandardCharsets.UTF_8),
                new ByteArrayOutputStream())
        );
    }

    @Test
    public void testSHA256Generator() throws IOException {
        InputStream is = IOUtils.toInputStream("something", StandardCharsets.UTF_8);
        String something = Hasher.createSHA256HashGenerator().hash(is);
        assertThat(something, is("3fc9b689459d738f8c88a3a48aa9e33542016b7a4052e001aaa536fca74813cb"));
    }

    @Test
    public void testMD5Generator() throws IOException {
        InputStream is = IOUtils.toInputStream("something", StandardCharsets.UTF_8);
        String something = Hasher.createMD5HashGenerator().hash(is);
        assertThat(something, is("437b930db84b8079c2dd804a71936b5f"));
    }

    @Test
    public void testSHA2562() {
        assertSHA(Hasher.calcSHA256("something"));
    }

    private void assertSHA(IRI calculated) {
        assertThat(calculated.getIRIString(), is("hash://sha256/3fc9b689459d738f8c88a3a48aa9e33542016b7a4052e001aaa536fca74813cb"));
    }

}