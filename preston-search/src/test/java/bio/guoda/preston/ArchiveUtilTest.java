package bio.guoda.preston;

import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.TestUtil;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.rdf.api.IRI;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

public class ArchiveUtilTest {

    private static String INDEX_TAR_GZ = "/bio/guoda/preston/index/tiny-lucene-index.tar.gz";
    private static String INDEX_UNPACKED = "/bio/guoda/preston/index/tiny-lucene-index";

    @Test
    public void copyDirectoryToBlobstoreAsTarGz() throws IOException {
        BlobStoreAppendOnly blobstore = createTestBlobStore();

        IRI version = ArchiveUtil.copyDirectoryToBlobstoreAsTarGz(blobstore, FileUtils.toFile(getClass().getResource(INDEX_UNPACKED)));

        assertThat(version.getIRIString(), is("hash://sha256/ebc0f76eafcfcbcc773e7f627964470896292b33c50ff600aa2520ca4e140fd9"));
    }

    @Test
    public void unpackTarGzFromBlobstore() throws IOException {
        BlobStoreReadOnly blobStore = TestUtil.getTestBlobStoreForResource(INDEX_TAR_GZ);
        File destination = FileUtils.getTempDirectory().toPath().resolve(UUID.randomUUID().toString()).toFile();

        ArchiveUtil.unpackTarGzFromBlobstore(
                blobStore,
                RefNodeFactory.toIRI("hash://sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                destination
        );

        assertTrue(destination.isDirectory());

        File unpackedDirectory = destination.toPath().resolve("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").toFile();
        assertTrue(unpackedDirectory.isDirectory());

        //noinspection DataFlowIssue
        Set<String> contents = Arrays.stream(unpackedDirectory.listFiles())
                .map(File::getName)
                .collect(Collectors.toSet());

        assertThat(contents.size(), is(5));
        assertThat(contents, hasItem("_0.cfe"));
        assertThat(contents, hasItem("_0.cfs"));
        assertThat(contents, hasItem("_0.si"));
        assertThat(contents, hasItem("segments_2"));
        assertThat(contents, hasItem("write.lock"));
    }

    private static BlobStoreAppendOnly createTestBlobStore() {
        return new BlobStoreAppendOnly(TestUtil.getTestPersistence(), false, HashType.sha256);
    }

}