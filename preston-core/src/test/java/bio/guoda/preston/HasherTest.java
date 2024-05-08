package bio.guoda.preston;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.rdf.api.IRI;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class HasherTest {

    @Test
    public void testSHA256() throws IOException {
        assertSHA(Hasher.calcHashIRI(
                IOUtils.toInputStream("something", StandardCharsets.UTF_8),
                new ByteArrayOutputStream(), HashType.sha256)
        );
    }

    @Test
    public void testSHA256AndMD5() throws IOException {
        InputStream is = IOUtils.toInputStream("something", StandardCharsets.UTF_8);
        OutputStream os = new ByteArrayOutputStream();
        boolean shouldCloseInputStream = true;
        Stream<HashType> hashTypes = Stream.of(HashType.values());
        List<IRI> IRIs = Hasher.calcHashIRIs(is, os, shouldCloseInputStream, hashTypes);

        assertThat(IRIs.size(), is(3));
        assertThat(IRIs.get(0).getIRIString(), is("hash://sha256/3fc9b689459d738f8c88a3a48aa9e33542016b7a4052e001aaa536fca74813cb"));
        assertThat(IRIs.get(1).getIRIString(), is("hash://md5/437b930db84b8079c2dd804a71936b5f"));
        assertThat(IRIs.get(2).getIRIString(), is("hash://sha1/1af17e73721dbe0c40011b82ed4bb1a7dbe3ce29"));
    }

    @Test
    public void testMD5() throws IOException {
        assertThat(Hasher.calcHashIRI(IOUtils.toInputStream("something", StandardCharsets.UTF_8), new ByteArrayOutputStream(), true, HashType.md5).getIRIString(),
                is("hash://md5/437b930db84b8079c2dd804a71936b5f"));
    }

    @Test
    public void testSHA1() throws IOException {
        assertThat(Hasher.calcHashIRI(IOUtils.toInputStream("something", StandardCharsets.UTF_8),
                new ByteArrayOutputStream(),
                true,
                HashType.sha1)
                        .getIRIString(),
                is("hash://sha1/1af17e73721dbe0c40011b82ed4bb1a7dbe3ce29"));
    }

    @Test
    public void testSHA256Generator() throws IOException {
        InputStream is = IOUtils.toInputStream("something", StandardCharsets.UTF_8);
        String something = createHashGenerator(HashType.sha256).hash(is);
        assertThat(something, is("hash://sha256/3fc9b689459d738f8c88a3a48aa9e33542016b7a4052e001aaa536fca74813cb"));
    }

    @Test
    public void testMD5Generator() throws IOException {
        InputStream is = IOUtils.toInputStream("something", StandardCharsets.UTF_8);
        String something = createHashGenerator(HashType.md5).hash(is);
        assertThat(something, is("hash://md5/437b930db84b8079c2dd804a71936b5f"));
    }

    @Test
    public void testSHA1Generator() throws IOException {
        InputStream is = IOUtils.toInputStream("something", StandardCharsets.UTF_8);
        String something = createHashGenerator(HashType.sha1).hash(is);
        assertThat(something, is("hash://sha1/1af17e73721dbe0c40011b82ed4bb1a7dbe3ce29"));
    }

    @Test
    public void testSHA2562() {
        assertSHA(Hasher.calcHashIRI("something", HashType.sha256));
    }

    private void assertSHA(IRI calculated) {
        assertThat(calculated.getIRIString(), is("hash://sha256/3fc9b689459d738f8c88a3a48aa9e33542016b7a4052e001aaa536fca74813cb"));
    }

    public static HashGenerator<String> createHashGenerator(HashType type) {
        return new HashGenerator<String>() {

            @Override
            public String hash(InputStream is) throws IOException {
                return hash(is, NullOutputStream.INSTANCE);
            }

            @Override
            public String hash(InputStream is, OutputStream os) throws IOException {
                return hash(is, os, true);
            }

            @Override
            public String hash(InputStream is, OutputStream os, boolean shouldCloseInputStream) throws IOException {
                return Hasher
                        .calcHashIRIs(
                                is,
                                os,
                                shouldCloseInputStream,
                                Stream.of(type))
                        .get(0)
                        .getIRIString();
            }
        };
    }


}