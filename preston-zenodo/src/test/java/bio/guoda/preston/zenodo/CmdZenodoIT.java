package bio.guoda.preston.zenodo;

import bio.guoda.preston.store.BlobStoreReadOnly;
import org.apache.commons.rdf.api.IRI;
import org.hamcrest.core.Is;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;


public class CmdZenodoIT {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void createOrUpdate() throws URISyntaxException, IOException {
        CmdZenodo cmdZenodo = new CmdZenodo();
        String resourceURI = "taxodros-data/4e/a9/4ea9ec0a300f006813340cc7ac85dfa5";

        File dataDir = folder.newFolder("zenodo-test");
        cmdZenodo.setLocalDataDir(dataDir.getAbsolutePath());
        cmdZenodo.setApiEndpoint("https://sandbox.zenodo.org");
        cmdZenodo.setAccessToken(ZenodoTestUtil.getAccessToken());

        cmdZenodo.setInputStream(getClass().getResourceAsStream(resourceURI));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        cmdZenodo.setOutputStream(outputStream);
        cmdZenodo.setCacheEnabled(false);

        AtomicInteger counter = new AtomicInteger();
        AtomicReference<IRI> requested = new AtomicReference<>(null);
        cmdZenodo.run(new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI uri) throws IOException {
                requested.set(uri);
                counter.incrementAndGet();
                return getClass().getResourceAsStream("taxodros/26/a2/26a2275383a5372f0f8d2e3852690ad6");
            }
        });

        assertThat(requested.get().getIRIString(), Is.is("hash://md5/26a2275383a5372f0f8d2e3852690ad6"));
        assertThat(counter.get(), Is.is(1));

    }

}