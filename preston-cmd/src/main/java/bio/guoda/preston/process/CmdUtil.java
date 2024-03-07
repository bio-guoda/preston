package bio.guoda.preston.process;

import bio.guoda.preston.cmd.ErrorChecking;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class CmdUtil {

    public static void print(String msg, OutputStream outputStream, LogErrorHandler handler) {
        try {
            String msgWithoutPadding = RegExUtils.removeAll(msg, Pattern.compile(EmittingStreamOfAnyQuad.DEFAULT_PREFIX_X_PRESTON));
            IOUtils.write(msgWithoutPadding.replaceAll("<> ", ""), outputStream, StandardCharsets.UTF_8);
            handleCheckError(outputStream, handler);
        } catch (IOException e) {
            handler.handleError();
        }
    }

    public static void handleCheckError(OutputStream outputStream, LogErrorHandler handler) {
        if (handler != null && outputStream != null) {
            if (outputStream instanceof PrintStream) {
                if (((PrintStream) outputStream).checkError()) {
                    handler.handleError();
                }
            } else if (outputStream instanceof ErrorChecking) {
                if (((ErrorChecking) outputStream).checkError()) {
                    handler.handleError();
                }
            }
        }
    }


}
