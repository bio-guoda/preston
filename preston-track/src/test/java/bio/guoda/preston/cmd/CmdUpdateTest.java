package bio.guoda.preston.cmd;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.BlobStore;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class CmdUpdateTest {

    @Test
    public void trackStdIn() throws IOException {
        AtomicReference<String> actual = new AtomicReference<>();
        BlobStore blobStore = new BlobStore() {
            @Override
            public InputStream get(IRI uri) throws IOException {
                return null;
            }

            @Override
            public IRI put(InputStream is) throws IOException {
                actual.set(IOUtils.toString(is, StandardCharsets.UTF_8));
                return RefNodeFactory.toIRI("foo:bar");
            }
        };
        HexaStoreNull logRelations = new HexaStoreNull();
        assertThat(logRelations.putLogVersionAttemptCount.get(), Is.is(0));

        CmdUpdate cmdUpdate = new CmdUpdate();
        cmdUpdate.setInputStream(IOUtils.toInputStream("hello world", StandardCharsets.UTF_8));
        cmdUpdate.run(
                blobStore,
                new BlobStoreNull(),
                logRelations
        );

        assertThat(actual.get(), is("hello world"));
    }

}