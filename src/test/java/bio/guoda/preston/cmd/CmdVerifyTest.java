package bio.guoda.preston.cmd;

import org.junit.Test;
import sun.misc.Signal;
import sun.misc.SignalHandler;

public class CmdVerifyTest {

    @Test
    public void pipeSignal() {
        Signal signal = new Signal("PIPE");
        Signal.handle(signal, new SignalHandler() {
            @Override
            public void handle(Signal signal) {
                throw new RuntimeException("got a: ["+ signal.getName() + "] with [" + signal.getNumber() + "]");
            }
        });

    }

}