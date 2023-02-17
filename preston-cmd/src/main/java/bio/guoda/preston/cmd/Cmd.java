package bio.guoda.preston.cmd;

import bio.guoda.preston.process.CmdUtil;
import bio.guoda.preston.process.LogErrorHandler;
import bio.guoda.preston.process.ProcessorState;
import org.apache.commons.io.output.ProxyOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;

public class Cmd implements ProcessorState {

    private AtomicBoolean shouldKeepProcessing = new AtomicBoolean(true);

    private OutputStream outputStream = new ProxyOutputStream(System.out) {
        @Override
        protected void afterWrite(int n) throws IOException {
            if (System.out.checkError()) {
                stopProcessing();
            }
        }


    };

    private InputStream inputStream = System.in;

//    static {
//        try {
//            Signal signal = new Signal("PIPE");
//            Signal.handle(signal, signal1 -> {
//                shouldKeepProcessing.set(false);
//            });
//        } catch (IllegalArgumentException ex) {
//            // ignore, probably not linux/posix system (e.g., windows)
//        }
//
//    }

    @Override
    public void stopProcessing() {
        System.out.println("stop processing");
        shouldKeepProcessing.set(false);
    }

    @Override
    public boolean shouldKeepProcessing() {
        return shouldKeepProcessing.get();
    }

    public OutputStream getOutputStream() {
        return outputStream;
    }

    public void print(String msg, LogErrorHandler handler) {
        CmdUtil.print(msg, getOutputStream(), handler);
    }

    public void setOutputStream(OutputStream outputStream) {
        this.outputStream = outputStream;
    }

    public InputStream getInputStream() {
        return inputStream;
    }

    public void setInputStream(InputStream inputStream) {
        this.inputStream = inputStream;
    }


}
