package bio.guoda.preston.cmd;

import bio.guoda.preston.DerefState;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.DerefProgressLogger;
import org.apache.commons.lang3.StringUtils;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;

public class DerefProgressLoggerTest {

    @Test
    public void logSomething() throws UnsupportedEncodingException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(out, true, StandardCharsets.UTF_8.name());

        DerefProgressLogger logger = new DerefProgressLogger(printStream);
        logger.setUpdateStepBytes(1);
        logger.onProgress(RefNodeFactory.toIRI("https://example.org"), DerefState.START, 10, 1024);
        logger.onProgress(RefNodeFactory.toIRI("https://example.org"), DerefState.BUSY, 10, 1024);
        logger.onProgress(RefNodeFactory.toIRI("https://example.org"), DerefState.BUSY, 20, 1024);
        logger.onProgress(RefNodeFactory.toIRI("https://example.org"), DerefState.DONE, 102, 1024);

        String[] lines = StringUtils.split(out.toString(StandardCharsets.UTF_8.name()), "\r\n");
        assertThat(lines[0], startsWith("<https://example.org> <http://purl.org/pav/sourceAccessedAt> \""));
        assertThat(lines[0], containsString("\"^^<http://www.w3.org/2001/XMLSchema#dateTime>"));
        assertThat(lines[0], endsWith("> ."));

        assertThat(lines[1], startsWith("1.0% of 1 kB at "));
        assertThat(lines[1], endsWith(" MB/s ETA: < 1 minute"));
        assertThat(lines[2], startsWith("2.0% of 1 kB at"));
        assertThat(lines[2], endsWith("MB/s ETA: < 1 minute"));

        assertThat(lines[3], startsWith("10.0% of 1 kB at"));
        assertThat(lines[3], endsWith("completed in < 1 minute"));

        assertThat(lines[4], startsWith("<https://example.org> <http://purl.org/pav/retrievedOn> \""));
        assertThat(lines[4], containsString("\"^^<http://www.w3.org/2001/XMLSchema#dateTime>"));
        assertThat(lines[4], endsWith("> ."));
    }

    @Test
    public void logSomethingLong() throws UnsupportedEncodingException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(out, true, StandardCharsets.UTF_8.name());

        DerefProgressLogger logger = new DerefProgressLogger(printStream);
        logger.setUpdateStepBytes(1);
        logger.onProgress(
                RefNodeFactory.toIRI("https://example.org/veryloooooooooooooooooooooooooooooooooooooooooooooooooooooooooong"),
                DerefState.START, 10, 1024);
        logger.onProgress(
                RefNodeFactory.toIRI("https://example.org/veryloooooooooooooooooooooooooooooooooooooooooooooooooooooooooong"),
                DerefState.BUSY, 10, 1024);

        String[] lines = StringUtils.split(out.toString(StandardCharsets.UTF_8.name()),"\r\n");

        assertThat(lines.length, Is.is(2));

        assertThat(lines[1], startsWith("1.0% of 1 kB at "));
        assertThat(lines[1], endsWith("ETA: < 1 minute"));
    }

    @Test
    public void logSomethingLongWithNoTotalLength() throws UnsupportedEncodingException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(out, true, StandardCharsets.UTF_8.name());

        DerefProgressLogger logger = new DerefProgressLogger(printStream);
        logger.setUpdateStepBytes(1);
        logger.onProgress(
                RefNodeFactory.toIRI("https://example.org/very"),
                DerefState.START, 1024, -1);
        logger.onProgress(
                RefNodeFactory.toIRI("https://example.org/very"),
                DerefState.BUSY, 1024, -1);

        String[] lines = StringUtils.split(out.toString(StandardCharsets.UTF_8.name()), "\r\n");
        assertThat(lines.length, Is.is(2));

        assertThat(lines[1], startsWith(
                "1 kB at "));
        assertThat(lines[1], endsWith(
                " MB/s"));


    }

}