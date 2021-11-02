package bio.guoda.preston.cmd;

import bio.guoda.preston.process.LogErrorHandler;
import org.junit.Ignore;
import org.junit.Test;

public class CmdListTest {

    @Ignore
    @Test
    public void run() {
        new CmdList().run(new LogErrorHandler() {
            @Override
            public void handleError() {
                // ignore
            }
        });
    }

}