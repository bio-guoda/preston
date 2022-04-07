
package bio.guoda.preston.store;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.process.StatementListener;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDFTerm;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertNotNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;

public class ProvenanceTrackerImplTest {

    @Test
    public void comesAfter() throws IOException {

        HexaStoreReadOnly hexastore = new HexaStoreReadOnly() {
            @Override
            public IRI get(Pair<RDFTerm, RDFTerm> queryKey) throws IOException {
                return getVersion(queryKey);
            }
        };

        ProvenanceTracker tracker = new ProvenanceTrackerImpl(hexastore);

        List<IRI> iris = new ArrayList<>();

        tracker.findDescendants(RefNodeConstants.BIODIVERSITY_DATASET_GRAPH, new StatementListener() {
            @Override
            public void on(Quad statement) {
                IRI version = VersionUtil.mostRecentVersionForStatement(statement);
                if (version != null) {
                    iris.add(version);
                }
            }
        });

        assertThat(iris.size(), Is.is(1));
        assertThat(iris.get(0), Is.is(RefNodeFactory.toIRI("some:iri")));
    }

    @Test
    public void comesAfter2() throws IOException {

        HexaStoreReadOnly hexastore = new HexaStoreReadOnly() {
            @Override
            public IRI get(Pair<RDFTerm, RDFTerm> queryKey) throws IOException {
                IRI iri = null;

                if (RefNodeFactory.toIRI(getOlder()).equals(queryKey.getValue())
                        && RefNodeConstants.HAS_PREVIOUS_VERSION.equals(queryKey.getKey())) {
                    iri = RefNodeFactory.toIRI(getNewer());
                } else {
                    iri = getVersion(queryKey);
                }

                return iri;
            }

        };

        ProvenanceTracker tracker = new ProvenanceTrackerImpl(hexastore);

        List<IRI> iris = new ArrayList<>();

        IRI someCurrent = RefNodeFactory.toIRI(getOlder());

        tracker.findDescendants(someCurrent, new StatementListener() {
            @Override
            public void on(Quad statement) {
                IRI version = VersionUtil.mostRecentVersionForStatement(statement);
                if (version != null) {
                    iris.add(version);
                }
            }
        });

        assertThat(iris.size(), Is.is(1));
        assertThat(iris.get(0), Is.is(RefNodeFactory.toIRI(getNewer())));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void cameBefore() throws IOException {
        new ProvenanceTrackerImpl(null, null)
                .traceOrigins(null, null);
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
        new ProvenanceTrackerImpl(null, blobStore)
                .traceOrigins(RefNodeFactory.toIRI("hash://sha256/8d1cbbfdbc366b4f2cf47dec44c9e20d7059e771037c3ff389dc44710280b66d"),
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
        new ProvenanceTrackerImpl(null, blobStore)
                .traceOrigins(RefNodeFactory.toIRI("hash://sha256/f663ab51cd63cce9598fd5b5782aa7638726347a6e8295f967b981fcf9481ad8"),
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
        new ProvenanceTrackerImpl(null, blobStore)
                .traceOrigins(RefNodeFactory.toIRI("hash://sha256/306ebe483d15970210add6552225835116f79ed78e66b08b170b2e761722f89d"),
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

    private IRI getVersion(Pair<RDFTerm, RDFTerm> queryKey) {
        IRI iri = null;
        if (RefNodeConstants.BIODIVERSITY_DATASET_GRAPH.equals(queryKey.getKey())
                && RefNodeConstants.HAS_VERSION.equals(queryKey.getValue())) {
            iri = RefNodeFactory.toIRI("some:iri");
        }
        return iri;
    }

    private String getOlder() {
        return "hash://sha256/f5851620a22110d6ebb73809df89c6321e79b4483dd2eb84ea77948505561463";
    }

    private String getNewer() {
        return "hash://sha256/77e30f34ca80fc7e2683e3953d0701a800862b2290d5617e8e5ef8230999e35f";
    }


}