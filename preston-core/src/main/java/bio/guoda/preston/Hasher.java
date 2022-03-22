package bio.guoda.preston;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.lang3.StringUtils;
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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    public static IRI calcSHA256(InputStream is, OutputStream os) throws IOException {
        return calcHashIRI(is, os, true, HashType.sha256.getAlgorithm());
    }

    public static IRI calcHashIRI(InputStream is, OutputStream os, boolean shouldCloseInputStream, String algorithm) throws IOException {
        List<IRI> iris = calcHashIRIs(
                is,
                os,
                shouldCloseInputStream,
                Stream.of(algorithm)
        );

        if (iris.size() != 1) {
            throw new IOException("expected 1 hash iri, but got [" + iris.size() + "]");
        }
        return iris.get(0);
    }

    public static IRI calcMD5(InputStream is, OutputStream os) throws IOException {
        return calcHashIRI(is, os, true, HashType.md5.getAlgorithm());
    }

    public static IRI calcSHA256(InputStream is, OutputStream os, boolean shouldCloseInputStream) throws IOException {
        return calcHashIRI(is, os, shouldCloseInputStream, HashType.sha256.getAlgorithm());
    }

    private static String toHashString64bit(MessageDigest md) {
        return toHashString(md, HashType.sha256);
    }

    private static String toHashString(MessageDigest md, HashType type) {
        return String.format("%0" + type.getHexLength() + "x", new BigInteger(1, md.digest()));
    }

    static String toHashString32bit(MessageDigest md) {
        return toHashString(md, HashType.md5);
    }

    public static IRI toSHA256IRI(MessageDigest md) {
        return toSHA256IRI(toHashString64bit(md));
    }

    private static IRI toMD5IRI(MessageDigest md) {
        return toMD5IRI(toHashString32bit(md));
    }

    public static MessageDigest createMessageDigest(
            InputStream is,
            OutputStream os,
            boolean shouldCloseInputStream,
            String hashAlgorithm)
            throws NoSuchAlgorithmException, IOException {

        Stream<String> algorithms = Stream.of(hashAlgorithm);

        List<MessageDigest> digests = streamIntoMessageDigests(is, os, shouldCloseInputStream, algorithms);
        if (digests.size() == 0) {
            throw new NoSuchAlgorithmException("failed to create hash for [" + StringUtils.join(algorithms, ";") + "]");
        }

        return digests.get(0);
    }

    public static List<MessageDigest> streamIntoMessageDigests(
            InputStream is,
            OutputStream os,
            boolean shouldCloseInputStream,
            Stream<String> algorithms) throws IOException {

        List<MessageDigest> digests = algorithms
                .map(algorithm -> {
                    try {
                        return MessageDigest.getInstance(algorithm);
                    } catch (NoSuchAlgorithmException e) {
                        throw new RuntimeException("failed to create digest for [" + algorithm + "]", e);
                    }
                }).collect(Collectors.toList());

        final AtomicReference<DigestInputStream> digestInputStream = new AtomicReference(null);
        digests
                .forEach(x -> {
                    DigestInputStream chainedInputStream = digestInputStream.get() == null
                            ? new DigestInputStream(is, x)
                            : new DigestInputStream(digestInputStream.get(), x);
                    digestInputStream.set(chainedInputStream);
                });


        IOUtils.copy(digestInputStream.get(), os);
        if (shouldCloseInputStream) {
            digestInputStream.get().close();
        }
        os.flush();
        os.close();
        return digests;
    }

    public static String getHashAlgorithm() {
        return HashType.sha256.getAlgorithm();
    }

    public static IRI toSHA256IRI(String sha256Hash) {
        return toHashIRI(HashType.sha256, sha256Hash);
    }

    public static IRI toMD5IRI(String hexHash) {
        return toHashIRI(HashType.md5, hexHash);
    }

    public static IRI toHashIRI(HashType type, String hash) {
        return RefNodeFactory.toIRI(URI.create(type.getPrefix() + hash));
    }

    public static List<IRI> calcHashIRIs(List<MessageDigest> messageDigests) {
        return messageDigests
                    .stream()
                    .map(md -> {
                        String algorithm = md.getAlgorithm();
                        Optional<IRI> hashIRI = Optional.empty();
                        if (StringUtils.equals(algorithm, HashType.sha256.getAlgorithm())) {
                            hashIRI = Optional.of(toSHA256IRI(md));
                        } else if (StringUtils.equals(algorithm, HashType.md5.getAlgorithm())) {
                            hashIRI = Optional.of(toMD5IRI(md));
                        }
                        return hashIRI;
                    })
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.toList());
    }

    public static List<IRI> calcHashIRIs(InputStream is, OutputStream os, boolean shouldCloseInputStream, Stream<String> algorithms) throws IOException {
        List<MessageDigest> messageDigests = streamIntoMessageDigests(
                is,
                os,
                shouldCloseInputStream,
                algorithms);

        return calcHashIRIs(messageDigests);
    }
}
