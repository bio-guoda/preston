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
    public void printMsgWithNamespacePadding() {
        assertPrintResult(
                "<http://n2t.net/ark:/65665/m34827bfe7-c154-4812-9a3b-470b08cdff1b> <http://purl.org/pav/hasVersion> <hash://sha256/24f010e52366737e48ad28c4b890a2fa83e1c9a7b6a7eb85c5f1cdb87de67d92> <f0fb6561-0646-42af-bf12-117d571189b1> .",
                "<http://n2t.net/ark:/65665/m34827bfe7-c154-4812-9a3b-470b08cdff1b> <http://purl.org/pav/hasVersion> <hash://sha256/24f010e52366737e48ad28c4b890a2fa83e1c9a7b6a7eb85c5f1cdb87de67d92> <x:preston:f0fb6561-0646-42af-bf12-117d571189b1> ."
        );
    }

    @Test
    public void printMsgWithoutNamespacePadding() {
        assertPrintResult("<foo> <bar> <abc> .", "<foo> <bar> <x:preston:abc> .");
    }

    @Test
    public void printMsgDefaultNamespace() {
        assertPrintResult("<foo> <bar> <abc> .", "<foo> <bar> <x:preston:abc> <x:preston:> .");
    }

    @Test
    public void printMsgWithNamespacePadding2() {
        assertPrintResult("<foo> <bar> <foo> <abc> .", "<foo> <bar> <foo> <x:preston:abc> .");
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