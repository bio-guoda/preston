package bio.guoda.preston.store;

import bio.guoda.preston.DerefProgressListener;
import bio.guoda.preston.RefNodeFactory;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

public class KeyValueStoreUtilIT {

    @Test
    public void progressDereferencingHttp() throws IOException {
        AtomicBoolean gotUpdate = new AtomicBoolean(false);

        DerefProgressListener listener = (dataURI, derefState, read, total) -> gotUpdate.set(true);

        Dereferencer<InputStream> derefStream = KeyValueStoreUtil.getDerefStream(URI.create("https://example.org"), listener, null);

        try (InputStream ignored = derefStream.get(RefNodeFactory.toIRI("https://example.org"))) {
            assertTrue(gotUpdate.get());
        }

    }
}