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
        String resourceURI = "taxodros-data/6e/f3/6ef3b8e326cd52972da1c00de60dc222";

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
                return getClass().getResourceAsStream("taxodros-data/7e/5a/7e5ae7ff14d66bff5224b21c80cdb87d");
            }
        });

        assertThat(requested.get().getIRIString(), Is.is("hash://md5/2b9b15fceffd48f29f58949373bebbd5"));
        assertThat(counter.get(), Is.is(2));

        System.out.println(new String(outputStream.toByteArray()));

    }

}