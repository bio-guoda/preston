package bio.guoda.preston.cmd;

import bio.guoda.preston.process.LogErrorHandler;
import bio.guoda.preston.process.ProcessorState;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;

public class Cmd implements ProcessorState {

    private AtomicBoolean shouldKeepProcessing = new AtomicBoolean(true);

    private OutputStream outputStream = System.out;

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
