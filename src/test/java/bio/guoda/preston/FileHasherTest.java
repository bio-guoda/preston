package bio.guoda.preston;

import bio.guoda.preston.store.KeyValueStoreLocalFileSystem;
import bio.guoda.preston.store.KeyTo5LevelPath;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.rdf.api.IRI;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class FileHasherTest {

    @Test
    public void sha256Binary() throws IOException {
        IRI shaIRI = Hasher.calcSHA256(dwcaInputStream(), new NullOutputStream());
        assertThat(shaIRI.getIRIString(), is("hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1"));
    }

    private InputStream dwcaInputStream() {
        return getClass().getResourceAsStream("plazidwca.zip");
    }

    @Test
    public void hashOfOrderedHashes() throws IOException {
        File tmpDir = new File("target/tmpDir");
        FileUtils.deleteQuietly(tmpDir);
        FileUtils.forceMkdir(tmpDir);
        File dataDir = new File("target/testDir");
        FileUtils.deleteQuietly(dataDir);
        FileUtils.forceMkdir(dataDir);

        List<String> expectedFiles = Arrays.asList("d2/d9/9a/d2d99ac0926397ebf0904c306730a1642ff10f43b3297777f749bbd720b0ba2e/data",
                "86/d9/ac/86d9acea28cbda1dffaea4131eff98a85ab40bfdb7de00a2c6ef3feac1baa798/data",
                "17/38/94/17389498c3e4a526a8ec2acb0c50cafde7a9fed070ca7d776c28701eef3f14f0/data",
                "d4/58/97/d458978d6cb4d4ba8585c722b57cf85099738e0b2165cb9d1f2d461fe82fec3e/data",
                "c3/f6/6a/c3f66a93c2e12eb64129cc5b194eae067891fde052a8143d39379d8ecb34f3cf/data",
                "e6/fd/d9/e6fdd97108175a3fac481f424f4da99ccefcb009149274dbcb0f46b55edde17b/data",
                "06/6e/68/066e685fa9b5ef8a01d7d69f2f0e7f4074a646e5ae437f741a4629d881ade9eb/data",
                "b9/b0/7a/b9b07a44c577c596e502ca764370d4cbd3cef06eff5b2c0b4dc5466a7614fd5a/data",
                "c8/68/83/c868839024230e2a0e144f6501da74502a9652fde035d53e2489cd92e77e1f59/data",
                "e1/ae/4a/e1ae4ab2711a209646d31b42d06698e242ecdf46e8d52394fb233d07337a222b/data");

        assertNoFilesExistYet(dataDir, expectedFiles);

        List<String> hashList = hashDWCA(tmpDir, dataDir, dwcaInputStream());


        assertThat(hashList.size(), is(10));

        assertFilesExist(dataDir, expectedFiles);

        List<String> expectedHashes = Arrays.asList(
                "hash://sha256/c3f66a93c2e12eb64129cc5b194eae067891fde052a8143d39379d8ecb34f3cf",
                "hash://sha256/17389498c3e4a526a8ec2acb0c50cafde7a9fed070ca7d776c28701eef3f14f0",
                "hash://sha256/d458978d6cb4d4ba8585c722b57cf85099738e0b2165cb9d1f2d461fe82fec3e",
                "hash://sha256/b9b07a44c577c596e502ca764370d4cbd3cef06eff5b2c0b4dc5466a7614fd5a",
                "hash://sha256/86d9acea28cbda1dffaea4131eff98a85ab40bfdb7de00a2c6ef3feac1baa798",
                "hash://sha256/c868839024230e2a0e144f6501da74502a9652fde035d53e2489cd92e77e1f59",
                "hash://sha256/066e685fa9b5ef8a01d7d69f2f0e7f4074a646e5ae437f741a4629d881ade9eb",
                "hash://sha256/d2d99ac0926397ebf0904c306730a1642ff10f43b3297777f749bbd720b0ba2e",
                "hash://sha256/e6fdd97108175a3fac481f424f4da99ccefcb009149274dbcb0f46b55edde17b",
                "hash://sha256/e1ae4ab2711a209646d31b42d06698e242ecdf46e8d52394fb233d07337a222b");

        assertThat(hashList, is(expectedHashes));

        Stream<String> sortedHashes = hashList.stream().distinct().sorted();
        String sortedJoin = sortedHashes.collect(Collectors.joining());
        IRI actualHashOfSortedHash = Hasher.calcSHA256(sortedJoin);

        assertThat(sortedJoin, startsWith("hash://sha256/066e685fa9b5ef8a01d7d69f2f0e7f4074a646e5ae437f741a4629d881ade9eb"));

        String expectedHashOfSortedHash = "hash://sha256/057391bd7ebe300dca56b8c2f44ec5526b8986bf832be552f285ad7d700aa1e6";
        assertThat(actualHashOfSortedHash.getIRIString(), is(expectedHashOfSortedHash));

        String joinedUnsorted = String.join("", hashList);

        IRI multihashUnsorted = Hasher.calcSHA256(joinedUnsorted);
        assertThat(multihashUnsorted.getIRIString(), is(not(expectedHashOfSortedHash)));


    }

    public void assertFilesExist(File dataDir, List<String> expectedFiles) {
        for (String fileName : expectedFiles) {
            assertTrue(new File(dataDir, fileName).exists());
        }
    }

    private List<String> hashDWCA(File tmpDir, File dataDir, InputStream dwcaInputStream) throws IOException {
        List<String> hashList = new ArrayList<String>();
        ZipInputStream is = new ZipInputStream(dwcaInputStream);
        ZipEntry entry;
        while ((entry = is.getNextEntry()) != null) {
            File tmpFile = new File(tmpDir, entry.getCrc() + ".tmp");
            OutputStream os = new FileOutputStream(tmpFile);
            String iriString = Hasher.calcSHA256(is, IOUtils.buffer(os)
                    , false).getIRIString();
            hashList.add(iriString);
            FileUtils.moveFile(tmpFile, KeyValueStoreLocalFileSystem.getDataFile(dataDir, new KeyTo5LevelPath().toPath(iriString)));
            is.closeEntry();
        }
        is.close();
        return hashList;
    }

    public void assertNoFilesExistYet(File dataDir, List<String> expectedFiles) {
        for (String fileName : expectedFiles) {
            assertFalse(new File(dataDir, fileName).exists());
        }
    }

}