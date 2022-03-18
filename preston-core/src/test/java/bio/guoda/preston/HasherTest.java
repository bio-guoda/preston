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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.stream.Stream;

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
    public void testSHA256AndMD5() throws IOException {
        InputStream is = IOUtils.toInputStream("something", StandardCharsets.UTF_8);
        OutputStream os = new ByteArrayOutputStream();
        boolean shouldCloseInputStream = true;
        Stream<String> algorithms = Stream.of("SHA-256", "MD5");
        List<IRI> IRIs = Hasher.calcHashIRIs(is, os, shouldCloseInputStream, algorithms);

        assertThat(IRIs.size(), is(2));
        assertThat(IRIs.get(0).getIRIString(), is("hash://sha256/3fc9b689459d738f8c88a3a48aa9e33542016b7a4052e001aaa536fca74813cb"));
        assertThat(IRIs.get(1).getIRIString(), is("hash://md5/437b930db84b8079c2dd804a71936b5f"));
    }

    @Test
    public void testMD5() throws IOException {
        assertThat(Hasher.calcMD5(
                IOUtils.toInputStream("something", StandardCharsets.UTF_8),
                new ByteArrayOutputStream()).getIRIString(),
                is("hash://md5/437b930db84b8079c2dd804a71936b5f"));
    }

    @Test
    public void testSHA256Generator() throws IOException {
        InputStream is = IOUtils.toInputStream("something", StandardCharsets.UTF_8);
        String something = createSHA256HashGenerator().hash(is);
        assertThat(something, is("hash://sha256/3fc9b689459d738f8c88a3a48aa9e33542016b7a4052e001aaa536fca74813cb"));
    }

    @Test
    public void testMD5Generator() throws IOException {
        InputStream is = IOUtils.toInputStream("something", StandardCharsets.UTF_8);
        String something = createMD5HashGenerator().hash(is);
        assertThat(something, is("437b930db84b8079c2dd804a71936b5f"));
    }

    @Test
    public void testSHA2562() {
        assertSHA(Hasher.calcSHA256("something"));
    }

    private void assertSHA(IRI calculated) {
        assertThat(calculated.getIRIString(), is("hash://sha256/3fc9b689459d738f8c88a3a48aa9e33542016b7a4052e001aaa536fca74813cb"));
    }

    public static HashGenerator<String> createSHA256HashGenerator() {
        return new HashGenerator<String>() {

            @Override
            public String hash(InputStream is) throws IOException {
                return hash(is, NullOutputStream.NULL_OUTPUT_STREAM);
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
                                Stream.of(HashType.sha256.getAlgorithm()))
                        .get(0)
                        .getIRIString();
            }
        };
    }

    private static HashGenerator<String> createMD5HashGenerator() {
        return new HashGenerator<String>() {

            @Override
            public String hash(InputStream is) throws IOException {
                return hash(is, NullOutputStream.NULL_OUTPUT_STREAM);
            }

            @Override
            public String hash(InputStream is, OutputStream os) throws IOException {
                return calcMD5String(is, os, true);
            }

            @Override
            public String hash(InputStream is, OutputStream os, boolean shouldCloseInputStream) throws IOException {
                return calcMD5String(is, os, shouldCloseInputStream);
            }
        };
    }

    static String calcMD5String(InputStream is, OutputStream os, boolean shouldCloseInputStream) throws IOException {
        try {
            MessageDigest md = Hasher.createMessageDigest(is, os, shouldCloseInputStream, HashType.md5.getAlgorithm());
            return Hasher.toHashString32bit(md);
        } catch (IOException | NoSuchAlgorithmException var9) {
            throw new IOException("failed to cache dataset", var9);
        }
    }



}