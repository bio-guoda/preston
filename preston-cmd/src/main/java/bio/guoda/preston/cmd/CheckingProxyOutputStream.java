package bio.guoda.preston.cmd;

import bio.guoda.preston.process.CmdUtil;
import bio.guoda.preston.process.ProcessorState;
import org.apache.commons.io.output.ProxyOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;

public class CheckingProxyOutputStream extends ProxyOutputStream implements ErrorChecking {
    private ProcessorState state;

    public CheckingProxyOutputStream(ProcessorState state, OutputStream out) {
        super(out);
        this.state = state;
    }

    @Override
    protected void afterWrite(int n) throws IOException {
        CmdUtil.handleCheckError(this.out, state::stopProcessing);
    }

    @Override
    public boolean checkError() {
        CmdUtil.handleCheckError(this.out, state::stopProcessing);
        return !state.shouldKeepProcessing();
    }

}
