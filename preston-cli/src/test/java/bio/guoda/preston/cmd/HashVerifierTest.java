package bio.guoda.preston.cmd;

import bio.guoda.preston.HashGeneratorImpl;
import bio.guoda.preston.HashType;
import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.KeyToPath;
import bio.guoda.preston.stream.ContentHashDereferencer;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
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
    public void innerHashBasedClaimPresentAndMatchesContent() throws IOException {
        BlobStoreReadOnly blobStore = contentBasedBlobStore();

        Quad statement = RefNodeFactory.toStatement(
                RefNodeFactory.toIRI("cut:hash://sha256/b5bb9d8014a0f9b1d61e21e796d78dccdf1352f23cd32812f4850b878ae4944c!/b1-2"),
                RefNodeConstants.HAS_VERSION,
                RefNodeFactory.toIRI("hash://sha256/9c3aee7110b787f0fb5f81633a36392bd277ea945d44c874a9a23601aefe20cf"));


        boolean containsHashBasedContentRelation = HashVerifier.containsHashBasedContentRelationClaim(statement);
        assertThat(containsHashBasedContentRelation, Is.is(true));

        boolean verified = HashVerifier.verifyContentRelationClaim(
                new ContentHashDereferencer(blobStore), (IRI) statement.getSubject(), (IRI) statement.getObject(), new HashGeneratorImpl(HashType.sha256));

        assertThat(verified, Is.is(true));
    }

    @Test
    public void innerHashBasedClaimPresentAndMatchesContentSubjectInnerhash() throws IOException {
        assertAvailableCompositeHash(
                RefNodeFactory.toIRI("https://api.zotero.org/groups/5435545/items/NDP3BCDT")
        );
    }

    @Test
    public void innerHashBasedClaimPresentAndMatchesContentSubjectInnerhashBlanknode() throws IOException {
        assertAvailableCompositeHash(
                RefNodeFactory.toBlank()
        );
    }

    private void assertAvailableCompositeHash(BlankNodeOrIRI subject) throws IOException {
        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI uri) throws IOException {
                String expectedInnerHash = "hash://md5/c5947e7e77c3be275090ffb69f4c83cd";
                if (StringUtils.equals(uri.getIRIString(),
                        RefNodeFactory.toIRI(expectedInnerHash).getIRIString())) {
                    return IOUtils.toInputStream("foo\n", StandardCharsets.UTF_8);
                } else {
                    throw new IOException("Kaboom!");
                }
            }
        };

        Quad statement = RefNodeFactory.toStatement(
                subject,
                RefNodeConstants.HAS_VERSION,
                RefNodeFactory.toIRI("cut:hash://md5/c5947e7e77c3be275090ffb69f4c83cd!/b1-2")
        );


        boolean containsHashBasedContentRelation = HashVerifier.containsHashBasedContentRelationClaim(statement);
        assertThat(containsHashBasedContentRelation, Is.is(false));

        boolean hasCompositeHash = HashVerifier.containsLocationBasedRelationClaimWithCompositeHash(statement);
        assertThat(hasCompositeHash, Is.is(true));

        boolean verified = HashVerifier.verifyCompositeHashIRI(
                new ContentHashDereferencer(blobStore),
                (IRI) statement.getObject(),
                new HashGeneratorImpl(HashType.sha256)
        );

        assertThat(verified, Is.is(true));
    }

    @Test
    public void innerHashBasedClaimPresentAndMatchesContentSubjectPlainHash() throws IOException {
        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI uri) throws IOException {
                String expectedInnerHash = "hash://md5/c5947e7e77c3be275090ffb69f4c83cd";
                if (StringUtils.equals(uri.getIRIString(),
                        RefNodeFactory.toIRI(expectedInnerHash).getIRIString())) {
                    return IOUtils.toInputStream("foo\n", StandardCharsets.UTF_8);
                } else {
                    throw new IOException("Kaboom!");
                }
            }
        };

        Quad statement = RefNodeFactory.toStatement(
                RefNodeFactory.toIRI("https://api.zotero.org/groups/5435545/items/NDP3BCDT"),
                RefNodeConstants.HAS_VERSION,
                RefNodeFactory.toIRI("hash://md5/c5947e7e77c3be275090ffb69f4c83cd")
        );


        boolean containsHashBasedContentRelation = HashVerifier.containsHashBasedContentRelationClaim(statement);
        assertThat(containsHashBasedContentRelation, Is.is(false));

        boolean hasCompositeHash = HashVerifier.containsLocationBasedRelationClaimWithCompositeHash(statement);
        assertThat(hasCompositeHash, Is.is(false));
    }

    @Test
    public void verifyHashBasedClaimPresentAndMatchesContent() throws IOException {
        String resourceLocation = getResourceLocation();
        BlobStoreReadOnly blobStore = contentBasedBlobStore();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        StatementsListener hashVerifier = new HashVerifier(
                new TreeMap<>(),
                blobStore,
                new HashGeneratorImpl(HashType.sha256),
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
                RefNodeFactory.toIRI("cut:hash://sha256/b5bb9d8014a0f9b1d61e21e796d78dccdf1352f23cd32812f4850b878ae4944c!/b1-2"),
                RefNodeConstants.HAS_VERSION,
                RefNodeFactory.toIRI("hash://sha256/9c3aee7110b787f0fb5f81633a36392bd277ea945d44c874a9a23601aefe20cf"))
        );

        assertThat(new String((outputStream).toByteArray(), StandardCharsets.UTF_8),
                Is.is("cut:hash://sha256/b5bb9d8014a0f9b1d61e21e796d78dccdf1352f23cd32812f4850b878ae4944c!/b1-2\t" +
                        resourceLocation + "\t" +
                        "OK\t" +
                        "CONTENT_PRESENT_VALID_HASH\t" +
                        "2\t" +
                        "hash://sha256/9c3aee7110b787f0fb5f81633a36392bd277ea945d44c874a9a23601aefe20cf\n"));

    }

    @Test
    public void verifyLocationBasedClaimOnCompositeHash() throws IOException {
        String resourceLocation = getResourceLocation();
        BlobStoreReadOnly blobStore = contentBasedBlobStore();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        StatementsListener hashVerifier = new HashVerifier(
                new TreeMap<>(),
                blobStore,
                new HashGeneratorImpl(HashType.sha256),
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
                RefNodeFactory.toIRI("https://api.zotero.org/groups/5435545/items/9CUCQ7BA"),
                RefNodeConstants.HAS_VERSION,
                RefNodeFactory.toIRI("cut:hash://sha256/b5bb9d8014a0f9b1d61e21e796d78dccdf1352f23cd32812f4850b878ae4944c!/b1-2"))
                );

        assertThat(new String((outputStream).toByteArray(), StandardCharsets.UTF_8),
                Is.is("cut:hash://sha256/b5bb9d8014a0f9b1d61e21e796d78dccdf1352f23cd32812f4850b878ae4944c!/b1-2\t" +
                        resourceLocation + "\t" +
                        "OK\t" +
                        "CONTENT_PRESENT_HASH_OPERATION_SUCCEEDS\t" +
                        "2\t" +
                        "hash://sha256/9c3aee7110b787f0fb5f81633a36392bd277ea945d44c874a9a23601aefe20cf\n"));

    }

    @Test
    public void innerHashBasedClaimPresentAndDoesNotMatchesContent() throws IOException {

        BlobStoreReadOnly blobStore = contentBasedBlobStore();

        Quad statement = RefNodeFactory.toStatement(
                RefNodeFactory.toIRI("cut:hash://sha256/b5bb9d8014a0f9b1d61e21e796d78dccdf1352f23cd32812f4850b878ae4944c!/b1-2"),
                RefNodeConstants.HAS_VERSION,
                RefNodeFactory.toIRI("hash://sha256/0c3aee7110b787f0fb5f81633a36392bd277ea945d44c874a9a23601aefe20cf"));

        boolean containsHashBasedContentRelation = HashVerifier.containsHashBasedContentRelationClaim(statement);
        assertThat(containsHashBasedContentRelation, Is.is(true));

        boolean verified = HashVerifier.verifyContentRelationClaim(
                new ContentHashDereferencer(blobStore),
                (IRI) statement.getSubject(),
                (IRI) statement.getObject(),
                new HashGeneratorImpl(HashType.sha256)
        );

        assertThat(verified, Is.is(false));
    }

    private BlobStoreReadOnly contentBasedBlobStore() {
        return new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI uri) throws IOException {
                String expectedInnerHash = "hash://sha256/b5bb9d8014a0f9b1d61e21e796d78dccdf1352f23cd32812f4850b878ae4944c";
                if (StringUtils.equals(uri.getIRIString(),
                        RefNodeFactory.toIRI(expectedInnerHash).getIRIString())) {
                    return IOUtils.toInputStream("foo\n", StandardCharsets.UTF_8);
                } else {
                    throw new IOException("Kaboom!");
                }
            }
        };
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