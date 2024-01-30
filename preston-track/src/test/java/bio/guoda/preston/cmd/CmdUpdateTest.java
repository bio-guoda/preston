package bio.guoda.preston.cmd;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.BlobStore;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import org.hamcrest.core.Is;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class CmdUpdateTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

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


    @Test
    public void trackFileByAbsolutePath() throws IOException {
        File foo = getDataFile("foo.txt");
        assertTrackedContent(foo.getAbsolutePath());
    }

    @Test
    public void trackFileByURIPath() throws IOException {
        File foo = getDataFile("foo.txt");
        assertTrackedContent(foo.toPath().toUri().toString());
    }

    private void assertTrackedContent(String dataLocation) throws IOException {

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

        File list = folder.newFile("list.txt");
        try (FileOutputStream listOs = new FileOutputStream(list)) {
            IOUtils.write(dataLocation, listOs, StandardCharsets.UTF_8);
        }

        CmdUpdate cmdUpdate = new CmdUpdate();
        cmdUpdate.setFilename(list.getAbsolutePath());
        cmdUpdate.run(
                blobStore,
                new BlobStoreNull(),
                logRelations
        );

        assertThat(actual.get(), is("hello world"));
    }

    private File getDataFile(String filename) throws IOException {
        File foo = folder.newFile(filename);
        try (FileOutputStream contentOs = new FileOutputStream(foo)) {
            IOUtils.write("hello world", contentOs, StandardCharsets.UTF_8);
        }
        return foo;
    }

}