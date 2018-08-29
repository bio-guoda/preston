package org.globalbioticinteractions.preston;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class Hasher {

    public static URI calcSHA256(String content) {
        try {
            return calcSHA256(IOUtils.toInputStream(content, StandardCharsets.UTF_8), new NullOutputStream());
        } catch (IOException e) {
            throw new IllegalStateException("unexpected failure of hash calculation", e);
        }
    }

    public static URI calcSHA256(InputStream is, OutputStream os) throws IOException {
        try {
            MessageDigest md = createDigest(is, os);
            String format = String.format("%064x", new BigInteger(1, md.digest()));
            return toHashURI(format);
        } catch (IOException | NoSuchAlgorithmException var9) {
            throw new IOException("failed to cache dataset", var9);
        }
    }

    private static MessageDigest createDigest(InputStream is, OutputStream os) throws NoSuchAlgorithmException, IOException {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        DigestInputStream digestInputStream = new DigestInputStream(is, md);
        IOUtils.copy(digestInputStream, os);
        digestInputStream.close();
        os.flush();
        os.close();
        return md;
    }

    public static URI toHashURI(String sha256Hash) {
        return URI.create(getHashPrefix() + sha256Hash);
    }

    public static String getHashPrefix() {
        return "hash://sha256/";
    }
}
