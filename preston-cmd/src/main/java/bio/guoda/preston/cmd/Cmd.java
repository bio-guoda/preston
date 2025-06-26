package bio.guoda.preston.cmd;

import bio.guoda.preston.process.CmdUtil;
import bio.guoda.preston.process.LogErrorHandler;
import bio.guoda.preston.process.ProcessorState;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public class Cmd implements ProcessorState {

    public static final String BUGS = "%nBugs:%n%nPlease report bugs or ask questions at" +
            "%n<https://github.com/bio-guoda/preston/issues>" +
            "%n or <mailto:info@globalbioticinteractions.org> .";
    private final List<ProcessorState> states = new ArrayList<ProcessorState>() {{
        add(new ProcessorStateImpl());
    }};

    private OutputStream outputStream = new CheckingProxyOutputStream(this, System.out);

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

    public void addState(ProcessorState state) {
        states.add(state);
    }

    @Override
    public void stopProcessing() {
        states.forEach(ProcessorState::stopProcessing);
    }

    @Override
    public boolean shouldKeepProcessing() {
        boolean keepProcessing = true;
        for (ProcessorState state : states) {
            if (!state.shouldKeepProcessing()) {
                keepProcessing = false;
                break;
            }
        }
        return keepProcessing;
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
