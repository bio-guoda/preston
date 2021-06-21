package bio.guoda.preston;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.rdf.api.IRI;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class FileHasherTest {

    @Test
    public void sha256Binary() throws IOException {
        IRI shaIRI = Hasher.calcSHA256(dwcaInputStream(), NullOutputStream.NULL_OUTPUT_STREAM);
        assertThat(shaIRI.getIRIString(), is("hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1"));
    }

    private InputStream dwcaInputStream() {
        return getClass().getResourceAsStream("plazidwca.zip");
    }

    @Test
    public void hashOfOrderedHashes() throws IOException {
        Set<String> hashList = hashDWCA(dwcaInputStream(), entry -> true);

        assertThat(hashList.size(), is(10));

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

        assertThat(hashList, is(new TreeSet<>(expectedHashes)));

        Stream<String> sortedHashes = hashList.stream().distinct().sorted();
        String sortedJoin = sortedHashes.collect(Collectors.joining());
        IRI actualHashOfSortedHash = Hasher.calcSHA256(sortedJoin);

        assertThat(sortedJoin, startsWith("hash://sha256/066e685fa9b5ef8a01d7d69f2f0e7f4074a646e5ae437f741a4629d881ade9eb"));

        String expectedHashOfSortedHash = "hash://sha256/057391bd7ebe300dca56b8c2f44ec5526b8986bf832be552f285ad7d700aa1e6";
        assertThat(actualHashOfSortedHash.getIRIString(), is(expectedHashOfSortedHash));

        String joinedUnsorted = String.join("", expectedHashes);

        IRI multihashUnsorted = Hasher.calcSHA256(joinedUnsorted);
        assertThat(multihashUnsorted.getIRIString(), is(not(expectedHashOfSortedHash)));
    }

    public interface EntryFilter {
        boolean accept(ZipEntry entry);
    }

    public static Set<String> hashDWCA(InputStream dwcaInputStream, EntryFilter filter) throws IOException {
        Set<String> hashList = new TreeSet<String>();
        ZipInputStream is = new ZipInputStream(dwcaInputStream);
        ZipEntry entry;
        while ((entry = is.getNextEntry()) != null) {
            if (filter.accept(entry)) {
                String iriString = Hasher.calcSHA256(is, NullOutputStream.NULL_OUTPUT_STREAM
                        , false).getIRIString();
                hashList.add(iriString);
            } else {
                IOUtils.copy(is, NullOutputStream.NULL_OUTPUT_STREAM);
            }
            is.closeEntry();
        }
        is.close();
        return hashList;
    }

}