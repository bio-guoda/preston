package bio.guoda.preston.cmd;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

class CopyShopImpl implements CopyShop {

    @Override
    public void copy(InputStream is, OutputStream os) throws IOException {
        IOUtils.copyLarge(is, os);
    }
}
