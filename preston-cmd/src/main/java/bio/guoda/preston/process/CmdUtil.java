package bio.guoda.preston.process;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

public class CmdUtil {

    public static void print(String msg, OutputStream outputStream, LogErrorHandler handler) {
        try {
            String msgWithoutPadding = RegExUtils.removeAll(msg, Pattern.compile(EmittingStreamOfAnyQuad.DEFAULT_PREFIX_X_PRESTON));
            IOUtils.write(msgWithoutPadding.replaceAll("<> ", ""), outputStream, StandardCharsets.UTF_8);
            checkError(outputStream, handler);
        } catch (IOException e) {
            handler.handleError();
        }
    }

    public static void checkError(OutputStream outputStream, LogErrorHandler handler) {
        if (handler != null) {
            if (outputStream instanceof PrintStream) {
                if (((PrintStream) outputStream).checkError()) {
                    handler.handleError();
                }
            }
        }
    }
}
