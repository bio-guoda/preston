package bio.guoda.preston.zenodo;

import bio.guoda.preston.store.Dereferencer;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.rdf.api.IRI;
import org.apache.http.entity.AbstractHttpEntity;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class DerferencingEntity extends AbstractHttpEntity {
    private Dereferencer<InputStream> dereferencer;
    private IRI resource;

    public DerferencingEntity(Dereferencer<InputStream> dereferencer, IRI resource) {
        this.dereferencer = dereferencer;
        this.resource = resource;
    }

    @Override
    public boolean isRepeatable() {
        return true;
    }

    @Override
    public long getContentLength() {
        try (InputStream inputStream = dereferencer.get(resource)) {
            return IOUtils.copy(inputStream, NullOutputStream.INSTANCE);
        } catch (IOException e) {
            return -1;
        }
    }

    @Override
    public InputStream getContent() throws IOException, UnsupportedOperationException {
        return dereferencer.get(resource);
    }

    @Override
    public void writeTo(OutputStream outStream) throws IOException {
        try (InputStream is = dereferencer.get(resource)) {
            IOUtils.copy(is, outStream);
        }
    }

    @Override
    public boolean isStreaming() {
        return true;
    }

}
