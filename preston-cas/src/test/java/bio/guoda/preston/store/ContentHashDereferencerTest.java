package bio.guoda.preston.store;

import bio.guoda.preston.stream.ContentStreamUtil;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.james.mime4j.Charsets;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicInteger;

import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.store.TestUtil.getTestBlobStoreForResource;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class ContentHashDereferencerTest {

    private String aContentHash = "hash://sha256/babababababababababababababababababababababababababababababababa";

    @Test
    public void getByteRange() throws IOException {
        BlobStoreReadOnly blobStore = key -> IOUtils.toInputStream("some bits and bytes", Charset.defaultCharset());
        InputStream content = new ContentHashDereferencer(blobStore).get(toIRI("cut:" + aContentHash + "!/b6-9"));
        assertThat(IOUtils.toString(content, Charsets.DEFAULT_CHARSET), is("bits"));
    }

    @Test
    public void getFileInArchive() throws IOException {
        BlobStoreReadOnly blobStore = getTestBlobStoreForResource("/bio/guoda/preston/process/nested.tar.gz");
        InputStream content = new ContentHashDereferencer(blobStore).get(toIRI("tar:gz:" + aContentHash + "!/level1.txt"));
        assertThat(IOUtils.toString(content, Charsets.DEFAULT_CHARSET), is("https://example.org"));
    }

    @Test
    public void getGzippedFile() throws IOException {
        BlobStoreReadOnly blobStore = getTestBlobStoreForResource("/bio/guoda/preston/process/hello.txt.gz");
        InputStream content = new ContentHashDereferencer(blobStore).get(toIRI("gz:" + aContentHash + ""));
        assertThat(IOUtils.toString(content, Charsets.DEFAULT_CHARSET), is("hello"));
    }

    @Test
    public void getGzippedFileIgnoreSuffix() throws IOException {
        BlobStoreReadOnly blobStore = getTestBlobStoreForResource("/bio/guoda/preston/process/hello.txt.gz");
        InputStream content = new ContentHashDereferencer(blobStore).get(toIRI("gz:" + aContentHash + "!/hello.txt"));
        assertThat(IOUtils.toString(content, Charsets.DEFAULT_CHARSET), is("hello"));
    }

    @Test
    public void getBytesFromFileInArchive() throws IOException {
        BlobStoreReadOnly blobStore = getTestBlobStoreForResource("/bio/guoda/preston/process/nested.tar.gz");
        InputStream content = new ContentHashDereferencer(blobStore).get(toIRI("cut:tar:gz:" + aContentHash + "!/level1.txt!/b9-15"));
        assertThat(IOUtils.toString(content, Charsets.DEFAULT_CHARSET), is("example"));
    }

    @Test
    public void getArchive() throws IOException {
        BlobStoreReadOnly blobStore = getTestBlobStoreForResource("/bio/guoda/preston/process/nested.tar.gz");

        final AtomicInteger bytesWritten = new AtomicInteger(0);
        OutputStream byteGobbler = new OutputStream() {
            @Override
            public void write(int b) {
                bytesWritten.incrementAndGet();
            }
        };

        InputStream content = new ContentHashDereferencer(blobStore).get(toIRI(aContentHash));
        IOUtils.copyLarge(content, byteGobbler);

        assertThat(bytesWritten.get(), is(303));
    }

    @Test
    public void extractContentHash() {
        IRI contentHash = ContentStreamUtil.extractContentHash(toIRI("blub:hash://sha256/babababababababababababababababababababababababababababababababa!/blah"));
        assertThat(contentHash.getIRIString(), is("hash://sha256/babababababababababababababababababababababababababababababababa"));
    }
}