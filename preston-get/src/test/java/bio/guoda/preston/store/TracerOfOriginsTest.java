
package bio.guoda.preston.store;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.cmd.ProcessorStateAlwaysContinue;
import bio.guoda.preston.process.StatementListener;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertNotNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;

public class TracerOfOriginsTest {

    @Test(expected = UnsupportedOperationException.class)
    public void cameBefore() throws IOException {
        new TracerOfOrigins(null, null)
                .trace(null, null);
    }

    @Test
    public void nothingCameBeforeRootVersion() throws IOException {
        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI uri) throws IOException {
                if (uri.equals(RefNodeFactory.toIRI("hash://sha256/8d1cbbfdbc366b4f2cf47dec44c9e20d7059e771037c3ff389dc44710280b66d"))) {
                    InputStream resourceAsStream = getClass().getResourceAsStream("versionRoot.nq");
                    assertNotNull(resourceAsStream);
                    return resourceAsStream;
                }
                throw new IOException("content unknown");
            }
        };

        List<Quad> versionStatements = new ArrayList<>();
        new TracerOfOrigins(blobStore, new ProcessorStateAlwaysContinue())
                .trace(RefNodeFactory.toIRI("hash://sha256/8d1cbbfdbc366b4f2cf47dec44c9e20d7059e771037c3ff389dc44710280b66d"),
                        new StatementListener() {
                            @Override
                            public void on(Quad statement) {
                                versionStatements.add(statement);
                            }
                        });

        assertThat(versionStatements.size(), Is.is(1));
        assertRootOrigin(versionStatements.get(0));

    }

    private void assertRootOrigin(Quad originStatement) {
        assertThat(originStatement.getSubject(), Is.is(RefNodeConstants.BIODIVERSITY_DATASET_GRAPH));
        assertThat(originStatement.getPredicate(), Is.is(RefNodeConstants.HAS_VERSION));
        assertThat(originStatement.getObject(), Is.is(RefNodeFactory.toIRI("hash://sha256/8d1cbbfdbc366b4f2cf47dec44c9e20d7059e771037c3ff389dc44710280b66d")));
        assertThat(originStatement.getGraphName().isPresent(), Is.is(false));
    }

    @Test
    public void somethingCameBeforeNonRootVersion() throws IOException {

        List<String> requested = new ArrayList<>();

        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI uri) throws IOException {
                requested.add(uri.getIRIString());
                if (uri.equals(RefNodeFactory.toIRI("hash://sha256/8d1cbbfdbc366b4f2cf47dec44c9e20d7059e771037c3ff389dc44710280b66d"))) {
                    return getClass().getResourceAsStream("versionRoot.nq");
                } else if (uri.equals(RefNodeFactory.toIRI("hash://sha256/f663ab51cd63cce9598fd5b5782aa7638726347a6e8295f967b981fcf9481ad8"))) {
                    return getClass().getResourceAsStream("versionNonRoot.nq");
                } else if (uri.equals(RefNodeFactory.toIRI("hash://sha256/306ebe483d15970210add6552225835116f79ed78e66b08b170b2e761722f89d"))) {
                    return getClass().getResourceAsStream("nonRDF.html");
                }
                throw new IOException("content unknown");
            }
        };

        List<Quad> versionStatements = new ArrayList<>();
        new TracerOfOrigins(blobStore, new ProcessorStateAlwaysContinue())
                .trace(RefNodeFactory.toIRI("hash://sha256/f663ab51cd63cce9598fd5b5782aa7638726347a6e8295f967b981fcf9481ad8"),
                        new StatementListener() {
                            @Override
                            public void on(Quad statement) {
                                versionStatements.add(statement);
                            }
                        });

        assertThat(requested.size(), Is.is(2));
        assertThat(requested, hasItems("hash://sha256/f663ab51cd63cce9598fd5b5782aa7638726347a6e8295f967b981fcf9481ad8", "hash://sha256/8d1cbbfdbc366b4f2cf47dec44c9e20d7059e771037c3ff389dc44710280b66d"));

        assertThat(versionStatements.size(), Is.is(2));
        assertThat(versionStatements.get(0).getSubject(), Is.is(RefNodeFactory.toIRI("hash://sha256/f663ab51cd63cce9598fd5b5782aa7638726347a6e8295f967b981fcf9481ad8")));
        assertThat(versionStatements.get(0).getPredicate(), Is.is(RefNodeConstants.WAS_DERIVED_FROM));
        assertThat(versionStatements.get(0).getObject(), Is.is(RefNodeFactory.toIRI("hash://sha256/8d1cbbfdbc366b4f2cf47dec44c9e20d7059e771037c3ff389dc44710280b66d")));
        assertThat(versionStatements.get(0).getGraphName().isPresent(), Is.is(false));

        assertRootOrigin(versionStatements.get(1));
    }

    @Test
    public void nonRDF() throws IOException {

        List<String> requested = new ArrayList<>();

        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI uri) throws IOException {
                requested.add(uri.getIRIString());
                if (uri.equals(RefNodeFactory.toIRI("hash://sha256/306ebe483d15970210add6552225835116f79ed78e66b08b170b2e761722f89d"))) {
                    return getClass().getResourceAsStream("nonRDF.html");
                }
                throw new IOException("content unknown");
            }
        };

        List<Quad> versionStatements = new ArrayList<>();
        new TracerOfOrigins(blobStore, new ProcessorStateAlwaysContinue())
                .trace(RefNodeFactory.toIRI("hash://sha256/306ebe483d15970210add6552225835116f79ed78e66b08b170b2e761722f89d"),
                        new StatementListener() {
                            @Override
                            public void on(Quad statement) {
                                versionStatements.add(statement);
                            }
                        });

        assertThat(requested.size(), Is.is(1));
        assertThat(requested, hasItems("hash://sha256/306ebe483d15970210add6552225835116f79ed78e66b08b170b2e761722f89d"));

        assertThat(versionStatements.size(), Is.is(0));
    }


}