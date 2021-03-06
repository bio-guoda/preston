package bio.guoda.preston;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.rdf.api.IRI;

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

    public static IRI calcSHA256(String content) {
        try {
            return calcSHA256(IOUtils.toInputStream(content, StandardCharsets.UTF_8), NullOutputStream.NULL_OUTPUT_STREAM);
        } catch (IOException e) {
            throw new IllegalStateException("unexpected failure of hash calculation", e);
        }
    }

    public static IRI calcSHA256(InputStream is) throws IOException {
        return calcSHA256(is, NullOutputStream.NULL_OUTPUT_STREAM, true);
    }

    public static HashGenerator<String> createSHA256HashGenerator() {
        return new HashGenerator<String>() {

            @Override
            public String hash(InputStream is) throws IOException {
                return hash(is, NullOutputStream.NULL_OUTPUT_STREAM);
            }

            @Override
            public String hash(InputStream is, OutputStream os) throws IOException {
                return calcSHA256String(is, os, true);
            }

            @Override
            public String hash(InputStream is, OutputStream os, boolean shouldCloseInputStream) throws IOException {
                return calcSHA256String(is, os, shouldCloseInputStream);
            }
        };
    }

    public static HashGenerator<IRI> createSHA256HashIRIGenerator() {
        return new HashGeneratorSHA256();
    }

    public static IRI calcSHA256(InputStream is, OutputStream os) throws IOException {
        return calcSHA256(is, os, true);
    }

    public static IRI calcSHA256(InputStream is, OutputStream os, boolean shouldCloseInputStream) throws IOException {
        return toSHA256IRI(calcSHA256String(is, os, shouldCloseInputStream));
    }

    public static String calcSHA256String(InputStream is, OutputStream os, boolean shouldCloseInputStream) throws IOException {
        try {
            MessageDigest md = createDigest(is, os, shouldCloseInputStream);
            return toHashString(md);
        } catch (IOException | NoSuchAlgorithmException var9) {
            throw new IOException("failed to cache dataset", var9);
        }
    }

    public static String toHashString(MessageDigest md) {
        return String.format("%064x", new BigInteger(1, md.digest()));
    }

    public static IRI toSHA256IRI(MessageDigest md) {
        return toSHA256IRI(toHashString(md));
    }

    private static MessageDigest createDigest(InputStream is, OutputStream os, boolean shouldCloseInputStream) throws NoSuchAlgorithmException, IOException {
        MessageDigest md = MessageDigest.getInstance(getHashAlgorithm());
        DigestInputStream digestInputStream = new DigestInputStream(is, md);
        IOUtils.copy(digestInputStream, os);
        if (shouldCloseInputStream) {
            digestInputStream.close();
        }
        os.flush();
        os.close();
        return md;
    }

    public static String getHashAlgorithm() {
        return "SHA-256";
    }

    public static IRI toSHA256IRI(String sha256Hash) {
        return toHashIRI(HashType.sha256, sha256Hash);
    }

    public static IRI toHashIRI(HashType type, String hash) {
        return RefNodeFactory.toIRI(URI.create(type.getPrefix() + hash));
    }

    public static String getHashPrefix() {
        return HashType.sha256.getPrefix();
    }

}
