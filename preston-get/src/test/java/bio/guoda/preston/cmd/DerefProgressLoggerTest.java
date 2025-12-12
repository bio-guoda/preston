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

        String[] newLines = StringUtils.split(out.toString(StandardCharsets.UTF_8.name()), "\n");
        assertThat(newLines[0], startsWith("<https://example.org> <http://purl.org/pav/sourceAccessedAt> \""));
        assertThat(newLines[0], containsString("\"^^<http://www.w3.org/2001/XMLSchema#dateTime>"));
        assertThat(newLines[0], endsWith("> ."));

        String[] processLines = StringUtils.split(newLines[1], "\r");


        assertThat(processLines[0], startsWith("1.0% of 1 kB at "));
        assertThat(processLines[0], endsWith(" MB/s ETA: < 1 minute"));
        assertThat(processLines[1], startsWith("2.0% of 1 kB at"));
        assertThat(processLines[1], endsWith("MB/s ETA: < 1 minute"));

        assertThat(processLines[2], startsWith("10.0% of 1 kB at"));
        assertThat(processLines[2], endsWith("completed in < 1 minute"));

        assertThat(newLines[2], startsWith("<https://example.org> <http://purl.org/pav/retrievedOn> \""));
        assertThat(newLines[2], containsString("\"^^<http://www.w3.org/2001/XMLSchema#dateTime>"));
        assertThat(newLines[2], endsWith("> ."));
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

        String[] newLines = StringUtils.split(out.toString(StandardCharsets.UTF_8.name()),'\n');

        assertThat(newLines.length, Is.is(2));

        String[] processLines = StringUtils.split(newLines[1], '\r');
        assertThat(processLines[0], startsWith("1.0% of 1 kB at "));
        assertThat(processLines[0], endsWith("ETA: < 1 minute"));
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

        String[] newLines = StringUtils.split(out.toString(StandardCharsets.UTF_8.name()), '\n');
        assertThat(newLines.length, Is.is(2));
        String[] processLines = StringUtils.split(newLines[1], '\r');

        assertThat(processLines[0], startsWith(
                "1 kB at "));
        assertThat(processLines[0], endsWith(
                " MB/s"));


    }

}