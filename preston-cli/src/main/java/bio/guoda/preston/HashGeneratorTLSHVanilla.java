package bio.guoda.preston;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class HashGeneratorTLSHVanilla extends HashGeneratorAbstract<String> {

    @Override
    public String hash(InputStream is, OutputStream os, boolean shouldCloseInputStream) throws IOException {
        return TLSHUtil.calculateLTSH(is, os, shouldCloseInputStream);
    }

}
