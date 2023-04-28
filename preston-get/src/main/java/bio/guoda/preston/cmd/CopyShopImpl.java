package bio.guoda.preston.cmd;

import bio.guoda.preston.process.ProcessorState;
import bio.guoda.preston.process.StopProcessingException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.ProxyInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

class CopyShopImpl implements CopyShop {

    private final ProcessorState state;

    public CopyShopImpl(ProcessorState state) {
        this.state = state;
    }

    @Override
    public void copy(InputStream is, OutputStream os) throws IOException {
        ProxyInputStream proxyInputStream = new ProxyInputStream(is) {
            @Override
            protected void afterRead(final int n) throws IOException {
                beforeRead(n);
            }

            @Override
            protected void beforeRead(final int n) throws IOException {
                if (!state.shouldKeepProcessing()) {
                    throw new StopProcessingException();
                }
            }

        };
        IOUtils.copyLarge(proxyInputStream, os);
    }
}
