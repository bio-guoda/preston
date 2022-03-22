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

    public static IRI calcHashIRI(String content, HashType hashType) {
        try {
            return calcHashIRI(IOUtils.toInputStream(content, StandardCharsets.UTF_8), NullOutputStream.NULL_OUTPUT_STREAM, hashType);
        } catch (IOException e) {
            throw new IllegalStateException("unexpected failure of hash calculation", e);
        }
    }

    public static IRI calcHashIRI(InputStream is) throws IOException {
        return calcHashIRI(is, NullOutputStream.NULL_OUTPUT_STREAM, true);
    }

    public static IRI calcHashIRI(InputStream is, OutputStream os, HashType type) throws IOException {
        return calcHashIRI(is, os, true, type);
    }

    public static IRI calcHashIRI(InputStream is, OutputStream os, boolean shouldCloseInputStream, HashType type) throws IOException {
        List<IRI> iris = calcHashIRIs(
                is,
                os,
                shouldCloseInputStream,
                Stream.of(type)
        );

        if (iris.size() != 1) {
            throw new IOException("expected 1 hash iri, but got [" + iris.size() + "]");
        }
        return iris.get(0);
    }

    public static IRI calcMD5(InputStream is, OutputStream os) throws IOException {
        return calcHashIRI(is, os, true, HashType.md5);
    }

    public static IRI calcHashIRI(InputStream is, OutputStream os, boolean shouldCloseInputStream) throws IOException {
        return calcHashIRI(is, os, shouldCloseInputStream, HashType.sha256);
    }

    public static String toHashString(MessageDigest md, HashType type) {
        return String.format("%0" + type.getHexLength() + "x", new BigInteger(1, md.digest()));
    }

    public static IRI toSHA256IRI(MessageDigest md) {
        return toSHA256IRI(toHashString(md, HashType.sha256));
    }

    private static IRI toMD5IRI(MessageDigest md) {
        return toMD5IRI(toHashString(md, HashType.md5));
    }


    public static List<MessageDigest> streamIntoMessageDigests(
            InputStream is,
            OutputStream os,
            boolean shouldCloseInputStream,
            Stream<HashType> algorithms) throws IOException {

        List<MessageDigest> digests = algorithms
                .map(hashType -> {
                    try {
                        return MessageDigest.getInstance(hashType.getAlgorithm());
                    } catch (NoSuchAlgorithmException e) {
                        throw new RuntimeException("failed to create digest for [" + hashType + "]", e);
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

    public static List<IRI> calcHashIRIs(InputStream is, OutputStream os, boolean shouldCloseInputStream, Stream<HashType> algorithms) throws IOException {
        List<MessageDigest> messageDigests = streamIntoMessageDigests(
                is,
                os,
                shouldCloseInputStream,
                algorithms);

        return calcHashIRIs(messageDigests);
    }
}
