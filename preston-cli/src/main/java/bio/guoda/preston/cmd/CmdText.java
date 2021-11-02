package bio.guoda.preston.cmd;

import bio.guoda.preston.TikaUtil;
import com.beust.jcommander.Parameters;

import static java.lang.System.exit;
import static org.apache.commons.io.IOUtils.read;

@Parameters(separators = "= ", commandDescription = "extract text from binary stdin")
public class CmdText extends CmdCat {

    @Override
    public void run() {
        try {
            TikaUtil.copyText(System.in, System.out);
        } catch (Throwable th) {
            th.printStackTrace(System.err);
            exit(1);
        }

        exit(0);
    }

}
