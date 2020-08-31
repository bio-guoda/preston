package bio.guoda.preston;

import org.apache.tika.io.IOUtils;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;

public class TikaUtilTest {

    @Test
    public void dwca2text() throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        TikaUtil.copyText(getClass().getResourceAsStream("/bio/guoda/preston/dwca-20180905.zip"), out);
        assertThat(new String(out.toByteArray(), StandardCharsets.UTF_8), containsString("Cerastes cerastes"));
    }

    @Test
    public void text2text() throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final InputStream is = IOUtils.toInputStream("hello", StandardCharsets.UTF_8.name());
        TikaUtil.copyText(is, out);
        assertThat(new String(out.toByteArray(), StandardCharsets.UTF_8),
                Is.is("hello\n"));
    }

    @Test
    public void rss2text() throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        TikaUtil.copyText(getClass().getResourceAsStream("/bio/guoda/preston/process/arthropodEasyCapture.xml"), out);
        assertThat(new String(out.toByteArray(), StandardCharsets.UTF_8),
                startsWith("Arthropod Easy Capture (AMNH)\n"));
    }

    @Test
    public void xml2text() throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        TikaUtil.copyText(getClass().getResourceAsStream("/bio/guoda/preston/process/biocase-datasets.xml"), out);
        assertThat(new String(out.toByteArray(), StandardCharsets.UTF_8),
                containsString(" OK\n" +
                        "   2018-08-31T22:29:51.910000\n"));
    }

}