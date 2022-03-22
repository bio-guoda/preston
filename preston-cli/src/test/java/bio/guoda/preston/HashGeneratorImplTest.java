package bio.guoda.preston;

import bio.guoda.preston.store.TestUtil;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class HashGeneratorImplTest {

    @Test
    public void hashTextFromResource() throws IOException {
        String name = "/bio/guoda/preston/process/idigbio-recordsets-complete.json";
        InputStream is = TestUtil.filterLineFeedFromTextInputStream(getClass().getResourceAsStream(name));
        IRI hash = new HashGeneratorImpl().hash(is);
        assertThat(hash.getIRIString(), is("hash://sha256/0931a831be557bfedd27b0068d9a0a9f14c1b92cbf9199e8ba79e04d0a6baedc"));
    }

    @Test
    public void hashTextFromResourceMD5() throws IOException {
        String name = "/bio/guoda/preston/process/idigbio-recordsets-complete.json";
        InputStream is = TestUtil.filterLineFeedFromTextInputStream(getClass().getResourceAsStream(name));
        IRI hash = new HashGeneratorImpl(HashType.md5).hash(is);
        assertThat(hash.getIRIString(), is("hash://md5/b5c57e251f776bb71b4c41fac5aee270"));
    }

    @Test
    public void hashTextFromString() throws IOException {
        IRI hash = new HashGeneratorImpl().hash(IOUtils.toInputStream("hello", StandardCharsets.UTF_8));
        assertThat(hash.getIRIString(), is("hash://sha256/2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"));
    };

    @Test
    public void hashBinary() throws IOException {
        IRI hash = new HashGeneratorImpl().hash(getClass().getResourceAsStream("/bio/guoda/preston/dwca-20180905.zip"));
        assertThat(hash.getIRIString(), is("hash://sha256/59f32445a50646d923f8ba462a7d87a848632f28bd93ac579de210e3375714de"));
    };

}