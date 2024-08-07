package bio.guoda.preston.stream;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.stream.LongStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;

public class SelectedLinesReaderTest {

    public static final Charset CHARSET = StandardCharsets.UTF_8;

    @Test
    public void readContiguous() throws IOException {
        InputStream in = IOUtils.toInputStream("1\n2\n3\n", CHARSET);
        SelectedLinesReader reader = new SelectedLinesReader(LongStream.of(1, 2).iterator(), new InputStreamReader(in, CHARSET));

        assertEquals('1', reader.read());
        assertEquals('\n', reader.read());
        assertEquals('2', reader.read());
        assertEquals('\n', reader.read());
        assertEquals(-1, reader.read());
    }

    @Test
    public void readContiguousMacOS() throws IOException {
        InputStream in = IOUtils.toInputStream("1\r2\r3\r", CHARSET);
        SelectedLinesReader reader = new SelectedLinesReader(LongStream.of(1, 2).iterator(), new InputStreamReader(in, CHARSET));

        assertEquals('1', reader.read());
        assertEquals('\r', reader.read());
        assertEquals('2', reader.read());
        assertEquals('\r', reader.read());
        assertEquals(-1, reader.read());
    }

    @Test
    public void readDisjointMacOS() throws IOException {
        InputStream in = IOUtils.toInputStream("1\r2\r3\r", CHARSET);
        SelectedLinesReader reader = new SelectedLinesReader(LongStream.of(1, 3).iterator(), new InputStreamReader(in, CHARSET));

        assertEquals('1', reader.read());
        assertEquals('\r', reader.read());
        assertEquals('3', reader.read());
        assertEquals('\r', reader.read());
        assertEquals(-1, reader.read());
    }

    @Test
    public void readContiguousDOS() throws IOException {
        InputStream in = IOUtils.toInputStream("1\r\n2\r\n3\r\n", CHARSET);
        SelectedLinesReader reader = new SelectedLinesReader(LongStream.of(1, 2).iterator(), new InputStreamReader(in, CHARSET));

        assertEquals('1', reader.read());
        assertEquals('\r', reader.read());
        assertEquals('\n', reader.read());
        assertEquals('2', reader.read());
        assertEquals('\r', reader.read());
        assertEquals('\n', reader.read());
        assertEquals(-1, reader.read());
    }

    @Test
    public void readDisjointDOS() throws IOException {
        InputStream in = IOUtils.toInputStream("1\r\n2\r\n3\r\n", CHARSET);
        SelectedLinesReader reader = new SelectedLinesReader(LongStream.of(1, 3).iterator(), new InputStreamReader(in, CHARSET));

        assertEquals('1', reader.read());
        assertEquals('\r', reader.read());
        assertEquals('\n', reader.read());
        assertEquals('3', reader.read());
        assertEquals('\r', reader.read());
        assertEquals('\n', reader.read());
        assertEquals(-1, reader.read());
    }

    @Test
    public void readDisjoint() throws IOException {
        InputStream in = IOUtils.toInputStream("1\n2\n3\n", CHARSET);
        SelectedLinesReader reader = new SelectedLinesReader(LongStream.of(1, 3).iterator(), new InputStreamReader(in, CHARSET));

        assertEquals('1', reader.read());
        assertEquals('\n', reader.read());
        assertEquals('3', reader.read());
        assertEquals('\n', reader.read());
        assertEquals(-1, reader.read());
    }

    @Test
    public void readDisjointInBulk() throws IOException {
        InputStream in = IOUtils.toInputStream("apple\nbeet\nscone\n", CHARSET);
        SelectedLinesReader reader = new SelectedLinesReader(LongStream.of(1, 3).iterator(), new InputStreamReader(in, CHARSET));

        char[] buffer = new char[16];
        int numBytesRead = reader.read(buffer, 0, 8);
        assertEquals(8, numBytesRead);
        assertEquals("apple\nsc", new String(buffer, 0, numBytesRead));
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void markAndReset() throws IOException {
        InputStream in = ContentStreamUtil.getMarkSupportedInputStream(IOUtils.toInputStream("1\n2\n3\n", CHARSET));
        SelectedLinesReader reader = new SelectedLinesReader(LongStream.of(1, 2, 3, 4).iterator(), new BufferedReader(new InputStreamReader(in, CHARSET)));

        char[] buffer = new char[16];
        reader.mark(10);
        int numBytesRead = reader.read(buffer, 0, 4);
        assertThat(numBytesRead, is(4));

        reader.reset();
        reader.read(buffer, 4, 6);
        assertThat(new String(buffer, 0, 10), is("1\n2\n1\n2\n3\n"));
    }

    @Test
    public void readFromEmptyStream() throws IOException {
        InputStream in = IOUtils.toInputStream("", CHARSET);
        SelectedLinesReader reader = new SelectedLinesReader(LongStream.of(1, 3).iterator(), new InputStreamReader(in, CHARSET));

        char[] buffer = new char[16];
        int numBytesRead = reader.read(buffer, 0, 8);
        assertEquals(-1, numBytesRead);
    }
}