package bio.guoda.preston.zenodo;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.Dereferencer;
import org.apache.commons.rdf.api.IRI;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.MatcherAssert.assertThat;

public class DereferencingEntityTest  {

    @Test
    public void countBytes() {
        DereferencingEntity dereferencingEntity = new DereferencingEntity(new Dereferencer<InputStream>() {
            @Override
            public InputStream get(IRI uri) throws IOException {
                return new ByteArrayInputStream("bla".getBytes(StandardCharsets.UTF_8));
            }
        }, RefNodeFactory.toIRI("https://example.org/data"));
        assertThat(dereferencingEntity.getContentLength(), Is.is(3L));
    }

    @Test
    public void countTonsOfBytes() {

        DereferencingEntity dereferencingEntity = new DereferencingEntity(new Dereferencer<InputStream>() {
            @Override
            public InputStream get(IRI uri) throws IOException {
                return new InputStream() {
                    long bytesRead = 0L;
                    @Override
                    public int read() throws IOException {
                        bytesRead = bytesRead + 1L;
                        return bytesRead < Integer.MAX_VALUE*2L ? 0 : -1;
                    }
                };
            }
        }, RefNodeFactory.toIRI("https://example.org/data"));
        assertThat(dereferencingEntity.getContentLength(), Is.is(4294967293L));
    }

}