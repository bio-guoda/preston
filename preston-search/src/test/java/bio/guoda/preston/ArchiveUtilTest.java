package bio.guoda.preston;

import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.TestUtil;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

public class ArchiveUtilTest {

    private static String NESTED_TAR_GZ = "/bio/guoda/preston/process/nested.tar.gz";

    @Test
    public void copyDirectoryToBlobstoreAsTarGz() {

    }

    @Test
    public void unpackTarGzFromBlobstore() throws IOException {
        BlobStoreReadOnly blobStore = TestUtil.getTestBlobStoreForResource(NESTED_TAR_GZ);
        File destination = FileUtils.getTempDirectory().toPath().resolve(UUID.randomUUID().toString()).toFile();

        ArchiveUtil.unpackTarGzFromBlobstore(
                blobStore,
                RefNodeFactory.toIRI("hash://sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                destination
        );

        assertTrue(destination.isDirectory());
        File[] firstLevelFiles = Objects.requireNonNull(destination.listFiles());
        assertThat(firstLevelFiles.length, is(37));
    }

    private static BlobStoreAppendOnly createTestBlobStore() {
        return new BlobStoreAppendOnly(TestUtil.getTestPersistence(), false, HashType.sha256);
    }

}