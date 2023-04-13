package bio.guoda.preston.process;

import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.MatcherAssert.assertThat;

public class CmdUtilTest {

    @Test
    public void printMsg() {
        assertPrintResult("hallo!", "hallo!");
    }

    @Test
    public void printMsgWithoutPadding() {
        assertPrintResult("abc", "x:preston:abc");
    }

    @Test
    public void printMsgWithoutNamespacePadding() {
        assertPrintResult("<foo> <bar>", "<foo> <x:preston:> <bar>" );
    }

    private void assertPrintResult(String expected, String provided) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        CmdUtil.print(provided, outputStream, new LogErrorHandler() {
            @Override
            public void handleError() {

            }
        });

        assertThat(new String(outputStream.toByteArray(), StandardCharsets.UTF_8), Is.is(expected));
    }

}