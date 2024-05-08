package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import bio.guoda.preston.Hasher;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.rdf.api.IRI;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.store.TestUtil.getTestBlobStoreForResource;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class ContentHashDereferencerTest {

    private String aContentHash = "hash://sha256/babababababababababababababababababababababababababababababababa";

    @Test
    public void getByteRange() throws IOException {
        BlobStoreReadOnly blobStore = key -> IOUtils.toInputStream("some bits and bytes", StandardCharsets.UTF_8);
        InputStream content = new ContentHashDereferencer(blobStore).get(toIRI("cut:" + aContentHash + "!/b6-9"));
        assertThat(IOUtils.toString(content, StandardCharsets.UTF_8), is("bits"));
    }

    @Test
    public void getFileInArchive() throws IOException {
        BlobStoreReadOnly blobStore = getTestBlobStoreForResource("/bio/guoda/preston/process/nested.tar.gz");
        InputStream content = new ContentHashDereferencer(blobStore).get(toIRI("tar:gz:" + aContentHash + "!/level1.txt"));
        assertThat(IOUtils.toString(content, StandardCharsets.UTF_8), is("https://example.org"));
    }


    @Test
    public void getFileInArchiveWithApacheVSF() throws IOException {



        BlobStoreReadOnly blobStore = getTestBlobStoreForResource("/bio/guoda/preston/process/nested.tar.gz");
        InputStream content = new ContentHashDereferencer(blobStore)
                .get(toIRI("tar:gz:" + aContentHash + "!/nested.tar!/level1.txt"));

        assertThat(
                IOUtils.toString(content, StandardCharsets.UTF_8),
                is("https://example.org")
        );
    }

    @Test
    public void getGzippedFile() throws IOException {
        BlobStoreReadOnly blobStore = getTestBlobStoreForResource("/bio/guoda/preston/process/hello.txt.gz");
        InputStream content = new ContentHashDereferencer(blobStore).get(toIRI("gz:" + aContentHash + ""));
        assertThat(IOUtils.toString(content, StandardCharsets.UTF_8), is("hello"));
    }

    @Test
    public void getGzippedFileIgnoreSuffix() throws IOException {
        BlobStoreReadOnly blobStore = getTestBlobStoreForResource("/bio/guoda/preston/process/hello.txt.gz");
        InputStream content = new ContentHashDereferencer(blobStore).get(toIRI("gz:" + aContentHash + "!/hello.txt"));
        assertThat(IOUtils.toString(content, StandardCharsets.UTF_8), is("hello"));
    }

    @Test
    public void getBytesFromFileInArchive() throws IOException {
        BlobStoreReadOnly blobStore = getTestBlobStoreForResource("/bio/guoda/preston/process/nested.tar.gz");
        InputStream content = new ContentHashDereferencer(blobStore).get(toIRI("cut:tar:gz:" + aContentHash + "!/level1.txt!/b9-15"));
        assertThat(IOUtils.toString(content, StandardCharsets.UTF_8), is("example"));
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
    public void getNODCArchiveExplicitTar() throws IOException {
        String urlString = "tar:gz:hash://sha256/bf18509ad6a2a97143d4f74e72dc4177ec31a4c50b3d7052f9a9cf6735f65e43!/50418.1.1.tar!/0050418/1.1/data/0-data/NODC_TaxonomicCode_V8_CD-ROM/TAXBRIEF.DAT";
        assertNODCContent(urlString);
    }

    @Test
    public void getNODCArchiveImplicitTar() throws IOException {
        String query = "tar:gz:hash://sha256/bf18509ad6a2a97143d4f74e72dc4177ec31a4c50b3d7052f9a9cf6735f65e43!/0050418/1.1/data/0-data/NODC_TaxonomicCode_V8_CD-ROM/TAXBRIEF.DAT";
        assertNODCContent(query);
    }

    private void assertNODCContent(String query) throws IOException {
        BlobStoreReadOnly blobStore = getTestBlobStoreForResource("/bio/guoda/preston/process/nodc.tar.gz");

        InputStream content = new ContentHashDereferencer(blobStore).get(toIRI(query));
        IRI iri = Hasher.calcHashIRI(content, NullOutputStream.INSTANCE, HashType.sha256);

        assertThat(iri.getIRIString(), is("hash://sha256/a908d1b21a86d95df40168df4795ad7c33ab668a383cb5944e6f4557e5186255"));
    }

}