package bio.guoda.preston.process;

import bio.guoda.preston.process.LogErrorHandler;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

public class CmdUtil  {

    public static void print(String msg, OutputStream outputStream, LogErrorHandler handler) {
        try {
            IOUtils.write(msg, outputStream, StandardCharsets.UTF_8);
            if (outputStream instanceof PrintStream) {
                if (((PrintStream) outputStream).checkError()) {
                    handler.handleError();
                }
            }
        } catch (IOException e) {
            handler.handleError();
        }
    }
}
