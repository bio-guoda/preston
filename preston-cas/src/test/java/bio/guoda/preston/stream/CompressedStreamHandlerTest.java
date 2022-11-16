package bio.guoda.preston.stream;

import bio.guoda.preston.RefNodeFactory;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;

import static org.hamcrest.MatcherAssert.assertThat;

public class CompressedStreamHandlerTest {

    @Test
    public void gzipConcat() throws ContentStreamException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();

        CompressedStreamHandler compressedStreamHandler = new CompressedStreamHandler(new ContentStreamHandler() {
            @Override
            public boolean handle(IRI version, InputStream in) throws ContentStreamException {
                try {
                    IOUtils.copy(in, os);
                } catch (IOException e) {
                    throw new ContentStreamException("unexpected", e);
                }
                return true;
            }

            @Override
            public boolean shouldKeepProcessing() {
                return false;
            }
        });

        compressedStreamHandler.handle(RefNodeFactory.toIRI("gz:foo:bar"), getClass().getResourceAsStream("foo4.txt.gz"));

        assertThat(new String(os.toByteArray(), StandardCharsets.UTF_8), Is.is(
                "bar\n" +
                        "bar2\n" +
                        "bar3bar\n" +
                        "bar2\n" +
                        "bar3"));
    }

    @Test

    public void plainGZIPInputStream() throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try (InputStream is = new GZIPInputStream(getClass().getResourceAsStream("foo4.txt.gz"))) {
            IOUtils.copy(is, os);
        }
        assertThat(new String(os.toByteArray(), StandardCharsets.UTF_8), Is.is(
                "bar\n" +
                        "bar2\n" +
                        "bar3bar\n" +
                        "bar2\n" +
                        "bar3"));
    }


}