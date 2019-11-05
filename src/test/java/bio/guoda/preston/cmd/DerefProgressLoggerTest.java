package bio.guoda.preston.cmd;

import bio.guoda.preston.DerefState;
import bio.guoda.preston.model.RefNodeFactory;
import org.apache.commons.lang3.StringUtils;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertThat;

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

        String[] lines = StringUtils.split(out.toString(StandardCharsets.UTF_8.name()), '\r');

        assertThat(lines[0], Is.is(
                "[https://example.org] 1.0% of 1 kB at ? MB/s ETA: < 1 minute"));
        assertThat(lines[1], startsWith("[https://example.org] 2.0% of 1 kB at"));
        assertThat(lines[1], endsWith("MB/s ETA: < 1 minute"));

        assertThat(lines[2], startsWith("[https://example.org] 10.0% of 1 kB at"));
        assertThat(lines[2], endsWith("completed in < 1 minute\n"));

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

        String[] lines = StringUtils.split(out.toString(StandardCharsets.UTF_8.name()), '\r');

        assertThat(lines[0], Is.is(
                "[https://example.org/very...ooooooooooooooooooooong] 1.0% of 1 kB at ? MB/s ETA: < 1 minute"));


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

        String[] lines = StringUtils.split(out.toString(StandardCharsets.UTF_8.name()), '\r');

        assertThat(lines[0], Is.is(
                "[https://example.org/very] 1 kB at ? MB/s"));


    }

}