package bio.guoda.preston.cmd;

import java.util.concurrent.atomic.AtomicBoolean;

public class Cmd implements ProcessorState {

    private static AtomicBoolean shouldKeepProcessing = new AtomicBoolean(true);

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
    public boolean shouldKeepProcessing() {
        return shouldKeepProcessing.get();
    }

}
