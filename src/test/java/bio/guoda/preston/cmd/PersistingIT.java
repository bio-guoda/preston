package bio.guoda.preston.cmd;

import bio.guoda.preston.DerefProgressListener;
import bio.guoda.preston.DerefState;
import bio.guoda.preston.model.RefNodeFactory;
import bio.guoda.preston.store.KeyValueStore;
import bio.guoda.preston.store.KeyValueStoreLocalFileSystemTest;
import org.apache.commons.rdf.api.IRI;
import org.apache.cxf.helpers.IOUtils;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class PersistingIT {

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