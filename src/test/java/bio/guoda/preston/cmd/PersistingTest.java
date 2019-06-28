package bio.guoda.preston.cmd;

import bio.guoda.preston.DerefProgressListener;
import bio.guoda.preston.DerefState;
import bio.guoda.preston.model.RefNodeFactory;
import org.apache.commons.rdf.api.IRI;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;

public class PersistingTest {

    @Test
    public void loggingDerefStream() throws IOException {
        AtomicBoolean gotUpdate = new AtomicBoolean(false);

        try (InputStream ignored = Persisting.getDerefStream(new DerefProgressListener() {
            @Override
            public void onProgress(IRI dataURI, DerefState derefState, long read, long total) {
                gotUpdate.set(true);
            }
        }).dereference(RefNodeFactory.toIRI("https://example.org"))) {
            assertTrue(gotUpdate.get());
        }

    }

}