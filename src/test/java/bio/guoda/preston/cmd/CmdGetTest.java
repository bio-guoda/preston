package bio.guoda.preston.cmd;

import bio.guoda.preston.process.BlobStoreReadOnly;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.util.Collections;

import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.store.TestUtil.getTestBlobStoreForResource;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class CmdGetTest {

    String aContentHash = "hash://sha256/babababababababababababababababababababababababababababababababa";

    @Test
    public void getSomething() {
        BlobStoreNull blobStoreNull = new BlobStoreNull(){
            @Override
            public InputStream get(IRI key) throws IOException {
                if (getAttemptCount.incrementAndGet() > 1 || !toIRI(aContentHash).equals(key)) {
                    throw new IOException("kaboom!");
                }
                return IOUtils.toInputStream("some bits and bytes", Charset.defaultCharset());
            }
        };

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        System.setOut(new PrintStream(out));

        CmdGet cmdGet = new CmdGet();
        cmdGet.setContentUris(Collections.singletonList(aContentHash));
        cmdGet.run(blobStoreNull);

        assertThat(blobStoreNull.getAttemptCount.get(), is(1));
        assertThat(out.toString(), is("some bits and bytes"));
    }

    @Test
    public void getByteRange() {
        BlobStoreReadOnly blobStore = key -> IOUtils.toInputStream("some bits and bytes", Charset.defaultCharset());

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        System.setOut(new PrintStream(out));

        CmdGet cmdGet = new CmdGet();
        cmdGet.setContentUris(Collections.singletonList("cut:" + aContentHash + "!/b6-9"));
        cmdGet.run(blobStore);

        assertThat(out.toString(), is("bits"));
    }

    @Test
    public void getBytesFromFileInArchive() {
        BlobStoreReadOnly blobStore = getTestBlobStoreForResource("/bio/guoda/preston/process/nested.tar.gz");

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        System.setOut(new PrintStream(out));

        CmdGet cmdGet = new CmdGet();
        cmdGet.setContentUris(Collections.singletonList("cut:tar:gz:" + aContentHash + "!/level1.txt!/b1-19"));
        cmdGet.run(blobStore);

        assertThat(out.toString(), is("https://example.org"));
    }
}