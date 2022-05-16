package bio.guoda.preston.cmd;

import bio.guoda.preston.HashGeneratorImpl;
import bio.guoda.preston.HashType;
import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.KeyToPath;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.TreeMap;

import static org.hamcrest.MatcherAssert.assertThat;

public class HashVerifierTest {

    @Test
    public void nonHashVersion() {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        StatementsListener hashVerifier = new HashVerifier(
                new TreeMap<>(),
                new BlobStoreNull(),
                null,
                true,
                outputStream,
                null);
        hashVerifier.on(RefNodeFactory.toStatement(
                RefNodeFactory.toIRI("foo:bar"),
                RefNodeConstants.HAS_VERSION,
                RefNodeFactory.toIRI("https://doi.org/10.5281/zenodo.1410543"))
        );

        assertThat(new String((outputStream).toByteArray(), StandardCharsets.UTF_8),
                Is.is("https://doi.org/10.5281/zenodo.1410543\t" +
                        "https://doi.org/10.5281/zenodo.1410543\t" +
                        "SKIP\t" +
                        "UNSUPPORTED_CONTENT_HASH\t" +
                        "\t" +
                        "\n"));
    }

    @Test
    public void hashVersionMissing() {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        StatementsListener hashVerifier = new HashVerifier(
                new TreeMap<>(),
                new BlobStoreNull(),
                null,
                true,
                outputStream,
                new KeyToPath() {
                    @Override
                    public URI toPath(IRI key) {
                        return URI.create("foo/bar");
                    }

                    @Override
                    public boolean supports(IRI key) {
                        return true;
                    }
                });
        hashVerifier.on(RefNodeFactory.toStatement(
                RefNodeFactory.toIRI("foo:bar"),
                RefNodeConstants.HAS_VERSION,
                RefNodeFactory.toIRI("hash://sha256/b5bb9d8014a0f9b1d61e21e796d78dccdf1352f23cd32812f4850b878ae4944c"))
        );

        assertThat(new String((outputStream).toByteArray(), StandardCharsets.UTF_8),
                Is.is("hash://sha256/b5bb9d8014a0f9b1d61e21e796d78dccdf1352f23cd32812f4850b878ae4944c\t" +
                        "foo/bar\t" +
                        "FAIL\t" +
                        "MISSING\t" +
                        "\t" +
                        "\n"));
    }

    @Test
    public void hashVersionPresentMismatchNotVerified() {
        String resourceLocation = getResourceLocation();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        StatementsListener hashVerifier = new HashVerifier(
                new TreeMap<>(),
                new BlobStoreReadOnly() {
                    @Override
                    public InputStream get(IRI uri) throws IOException {
                        return IOUtils.toInputStream("NOTfoo\n", StandardCharsets.UTF_8);
                    }
                },
                null,
                true,
                outputStream,
                new KeyToPath() {
                    @Override
                    public URI toPath(IRI key) {
                        return URI.create(resourceLocation);
                    }

                    @Override
                    public boolean supports(IRI key) {
                        return true;
                    }
                });
        hashVerifier.on(RefNodeFactory.toStatement(
                RefNodeFactory.toIRI("foo:bar"),
                RefNodeConstants.HAS_VERSION,
                RefNodeFactory.toIRI("hash://sha256/b5bb9d8014a0f9b1d61e21e796d78dccdf1352f23cd32812f4850b878ae4944c"))
        );

        assertThat(new String((outputStream).toByteArray(), StandardCharsets.UTF_8),
                Is.is("hash://sha256/b5bb9d8014a0f9b1d61e21e796d78dccdf1352f23cd32812f4850b878ae4944c\t" +
                        resourceLocation + "\t" +
                        "OK\t" +
                        "CONTENT_PRESENT_HASH_NOT_VERIFIED\t" +
                        "4384\t" +
                        "\n"));
    }

    @Test
    public void hashVersionPresentMismatchVerified() {
        String resourceLocation = getResourceLocation();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        StatementsListener hashVerifier = new HashVerifier(
                new TreeMap<>(),
                new BlobStoreReadOnly() {
                    @Override
                    public InputStream get(IRI uri) throws IOException {
                        return IOUtils.toInputStream("NOTfoo\n", StandardCharsets.UTF_8);
                    }
                },
                new HashGeneratorImpl(HashType.sha256),
                false,
                outputStream,
                new KeyToPath() {
                    @Override
                    public URI toPath(IRI key) {
                        return URI.create(resourceLocation);
                    }

                    @Override
                    public boolean supports(IRI key) {
                        return true;
                    }
                });
        hashVerifier.on(RefNodeFactory.toStatement(
                RefNodeFactory.toIRI("foo:bar"),
                RefNodeConstants.HAS_VERSION,
                RefNodeFactory.toIRI("hash://sha256/b5bb9d8014a0f9b1d61e21e796d78dccdf1352f23cd32812f4850b878ae4944c"))
        );

        assertThat(new String((outputStream).toByteArray(), StandardCharsets.UTF_8),
                Is.is("hash://sha256/b5bb9d8014a0f9b1d61e21e796d78dccdf1352f23cd32812f4850b878ae4944c\t" +
                        resourceLocation + "\t" +
                        "FAIL\t" +
                        "CONTENT_PRESENT_INVALID_HASH\t" +
                        "7\t" +
                        "hash://sha256/240bbad6f38fa5f3e153c03d469f01284fedeab4f483f63a1d504e77af8daf12\n"));
    }


    @Test
    public void hashVersionPresentHashVerified() {
        String resourceLocation = getResourceLocation();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        StatementsListener hashVerifier = new HashVerifier(
                new TreeMap<>(),
                new BlobStoreReadOnly() {
                    @Override
                    public InputStream get(IRI uri) throws IOException {
                        return new FileInputStream(new File(URI.create(resourceLocation)));
                    }
                },
                new HashGeneratorImpl(HashType.sha256),
                false,
                outputStream,
                new KeyToPath() {
                    @Override
                    public URI toPath(IRI key) {
                        return URI.create(resourceLocation);
                    }

                    @Override
                    public boolean supports(IRI key) {
                        return true;
                    }
                });
        hashVerifier.on(RefNodeFactory.toStatement(
                RefNodeFactory.toIRI("foo:bar"),
                RefNodeConstants.HAS_VERSION,
                RefNodeFactory.toIRI("hash://sha256/7d8ae37f029425e5b90446fb3c3d9f7faec57487c49c2c0dbf9f0964224f446e"))
        );

        assertThat(new String((outputStream).toByteArray(), StandardCharsets.UTF_8),
                Is.is("hash://sha256/7d8ae37f029425e5b90446fb3c3d9f7faec57487c49c2c0dbf9f0964224f446e\t" +
                        resourceLocation + "\t" +
                        "OK\t" +
                        "CONTENT_PRESENT_VALID_HASH\t" +
                        "4384\t" +
                        "hash://sha256/7d8ae37f029425e5b90446fb3c3d9f7faec57487c49c2c0dbf9f0964224f446e\n"));
    }

    String getResourceLocation() {
        return getClass().getResource("verify.zip").toExternalForm();
    }


}