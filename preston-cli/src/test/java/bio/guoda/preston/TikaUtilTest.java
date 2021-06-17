package bio.guoda.preston;

import org.apache.commons.io.output.CountingOutputStream;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.tika.Tika;
import org.apache.tika.io.CountingInputStream;
import org.apache.tika.io.IOUtils;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;

public class TikaUtilTest {

    @Test
    public void dwca2text() throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (final InputStream resourceAsStream = getClass().getResourceAsStream("/bio/guoda/preston/dwca-20180905.zip")) {
            TikaUtil.copyText(resourceAsStream, out);
        }
        assertThat(new String(out.toByteArray(), StandardCharsets.UTF_8), containsString("Cerastes cerastes"));
    }

    @Test
    public void text2text() throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (final InputStream is = IOUtils.toInputStream("hello", StandardCharsets.UTF_8.name())) {
            TikaUtil.copyText(is, out);
        }
        assertThat(new String(out.toByteArray(), StandardCharsets.UTF_8),
                Is.is("hello\n"));
    }

    @Test
    public void rss2text() throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (final InputStream resourceAsStream = getClass().getResourceAsStream("/bio/guoda/preston/process/arthropodEasyCapture.xml")) {
            TikaUtil.copyText(resourceAsStream, out);
        }
        assertThat(new String(out.toByteArray(), StandardCharsets.UTF_8),
                startsWith("Arthropod Easy Capture (AMNH)\n"));
    }

    @Test
    public void xml2text() throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (final InputStream is = getClass().getResourceAsStream("/bio/guoda/preston/process/biocase-datasets.xml")) {
            TikaUtil.copyText(is, out);
        }
        assertThat(new String(out.toByteArray(), StandardCharsets.UTF_8),
                containsString(" OK\n" +
                        "   2018-08-31T22:29:51.910000\n"));
    }

    @Test
    public void detectImage() throws IOException {
        InputStream resourceAsStream = getClass().getResourceAsStream("MCZ_Ent_17219.jpg");
        CountingInputStream countingIs = new CountingInputStream(resourceAsStream);
        String detect = new Tika()
                .detect(countingIs);
        assertThat(detect, Is.is("image/jpeg"));
        assertThat(countingIs.getCount(), Is.is(66562));

        CountingOutputStream output = new CountingOutputStream(NullOutputStream.NULL_OUTPUT_STREAM);
        IOUtils.copy(getClass().getResourceAsStream("MCZ_Ent_17219.jpg"), output);


        assertThat(output.getCount(), is(176236));

        assertThat(countingIs.getCount(), lessThan(output.getCount()));
    }

}