package bio.guoda.preston.zenodo;

import bio.guoda.preston.store.DerefProgressLogger;
import bio.guoda.preston.store.Dereferencer;
import bio.guoda.preston.stream.ContentStreamUtil;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.CountingInputStream;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.rdf.api.IRI;
import org.apache.http.entity.AbstractHttpEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicLong;

public class DereferencingEntity extends AbstractHttpEntity {

    private static final Logger LOG = LoggerFactory.getLogger(DereferencingEntity.class);

    private Dereferencer<InputStream> dereferencer;
    private IRI resource;
    private final AtomicLong contentLength = new AtomicLong(-2);

    public DereferencingEntity(Dereferencer<InputStream> dereferencer, IRI resource) {
        this.dereferencer = dereferencer;
        this.resource = resource;
    }

    @Override
    public boolean isRepeatable() {
        return true;
    }

    @Override
    public long getContentLength() {
        if (contentLength.get() < -1) {
            try (InputStream inputStream = dereferencer.get(resource)) {
                LOG.info("calculating content length for [" + resource.getIRIString() + "]...");
                DerefProgressLogger outgoing = new DerefProgressLogger(System.err, "counting bytes: ");
                InputStream inputStreamWithLogger = ContentStreamUtil.getInputStreamWithProgressLogger(resource, outgoing, -1, inputStream);
                int copy = IOUtils.copy(inputStreamWithLogger, NullOutputStream.INSTANCE);
                LOG.info("calculating content length for [" + resource.getIRIString() + "] done.");
                contentLength.set(copy);
            } catch (IOException e) {
                contentLength.set(-1);
            }
        }
        return contentLength.get();
    }

    @Override
    public InputStream getContent() throws IOException, UnsupportedOperationException {
        return dereferencer.get(resource);
    }

    @Override
    public void writeTo(OutputStream outStream) throws IOException {
        LOG.info("attempting to dereference [" + resource.getIRIString() + "]");

        try (InputStream is = dereferencer.get(resource)) {
            DerefProgressLogger outgoing = new DerefProgressLogger(System.err, "uploading: ");
            OutputStream outputStreamWithLogger = ContentStreamUtil.getOutputStreamWithProgressLogger(resource, outgoing, getContentLength(), outStream);
            IOUtils.copy(is, outputStreamWithLogger);
            outputStreamWithLogger.flush();
            LOG.info("copying [" + resource.getIRIString() + "] complete.");
        }
    }

    @Override
    public boolean isStreaming() {
        return true;
    }

}
