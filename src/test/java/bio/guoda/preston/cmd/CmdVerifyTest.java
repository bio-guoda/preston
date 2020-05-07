package bio.guoda.preston.cmd;

import bio.guoda.preston.HashType;
import org.junit.Test;

public class CmdVerifyTest {

    @Test
    public void pipeSignal() {
//        Signal signal = new Signal("PIPE");
//        Signal.handle(signal, new SignalHandler() {
//            @Override
//            public void handle(Signal signal) {
//                throw new RuntimeException("got a: ["+ signal.getName() + "] with [" + signal.getNumber() + "]");
//            }
//        });

    }

    @Test
    public void verifySha256() {
        new CmdVerify().run();
    }

    @Test
    public void verifyShaLTSH() {
        CmdVerify cmdVerify = new CmdVerify();
        cmdVerify.setHashType(HashType.TLSH);
        cmdVerify.run();
    }

}