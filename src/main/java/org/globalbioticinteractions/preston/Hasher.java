package org.globalbioticinteractions.preston;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class Hasher {

    public static String calcSHA256(String str) throws IOException {
        return calcSHA256(IOUtils.toInputStream(str, StandardCharsets.UTF_8), new NullOutputStream());
    }

    public static String calcSHA256(InputStream is, OutputStream os) throws IOException {
        try {
            MessageDigest md = createDigest(is, os);
            return String.format("%064x", new BigInteger(1, md.digest()));
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
}
