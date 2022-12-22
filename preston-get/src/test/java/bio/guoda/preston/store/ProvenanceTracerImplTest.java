
package bio.guoda.preston.store;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.process.ProcessorStateAlwaysContinue;
import bio.guoda.preston.process.StatementListener;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.hamcrest.core.Is;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertNotNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;

public class ProvenanceTracerImplTest {

    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();


    @Test(expected = UnsupportedOperationException.class)
    public void cameBefore() throws IOException {
        new ProvenanceTracerImpl(null, null)
                .trace(null, null);
    }

    @Test
    public void nothingCameBeforeRootVersion() throws IOException {
        KeyValueStoreReadOnly blobStore = new KeyValueStoreReadOnly() {
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
        new ProvenanceTracerImpl(blobStore, new ProcessorStateAlwaysContinue())
                .trace(RefNodeFactory.toIRI("hash://sha256/8d1cbbfdbc366b4f2cf47dec44c9e20d7059e771037c3ff389dc44710280b66d"),
                        new StatementListener() {
                            @Override
                            public void on(Quad statement) {
                                versionStatements.add(statement);
                            }
                        });

        assertThat(versionStatements.size(), Is.is(1));
        assertRootOrigin(versionStatements.get(0), "hash://sha256/8d1cbbfdbc366b4f2cf47dec44c9e20d7059e771037c3ff389dc44710280b66d");

    }

    private void assertRootOrigin(Quad originStatement, String provenanceAnchor) {
        assertThat(originStatement.getSubject(), Is.is(RefNodeConstants.BIODIVERSITY_DATASET_GRAPH));
        assertThat(originStatement.getPredicate(), Is.is(RefNodeConstants.HAS_VERSION));
        assertThat(originStatement.getObject(), Is.is(RefNodeFactory.toIRI(provenanceAnchor)));
        assertThat(originStatement.getGraphName().isPresent(), Is.is(false));
    }

    @Test
    public void somethingCameBeforeNonRootVersion() throws IOException {

        List<String> requested = new ArrayList<>();

        KeyValueStoreReadOnly blobStore = new KeyValueStoreReadOnly() {
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
        new ProvenanceTracerImpl(blobStore, new ProcessorStateAlwaysContinue())
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

        assertRootOrigin(versionStatements.get(1), "hash://sha256/8d1cbbfdbc366b4f2cf47dec44c9e20d7059e771037c3ff389dc44710280b66d");
    }


    @Test
    public void tracingMergedOriginsNoRemote() throws IOException, URISyntaxException {
        KeyValueStore graphB = getKeyValueStoreForGraph("b/");

        assertNotNull(graphB.get(RefNodeFactory.toIRI("hash://sha256/0cfebefb2c2de0de893ab11071dcbaac2b75c217a566a1f3739577b9b12265e8")));

        List<String> notFound = new ArrayList<>();

        KeyValueStoreReadOnly store = new KeyValueStoreWithFallback(graphB, new KeyValueStoreReadOnly() {
            @Override
            public InputStream get(IRI uri) throws IOException {
                notFound.add(uri.getIRIString());
                return null;
            }
        });

        List<Quad> versionStatements = new ArrayList<>();
        new ProvenanceTracerImpl(store, new ProcessorStateAlwaysContinue())
                .trace(RefNodeFactory.toIRI("hash://sha256/0cfebefb2c2de0de893ab11071dcbaac2b75c217a566a1f3739577b9b12265e8"),
                        new StatementListener() {
                            @Override
                            public void on(Quad statement) {
                                versionStatements.add(statement);
                            }
                        });

        assertThat(notFound.size(), Is.is(1));
        assertThat(notFound.get(0), Is.is("hash://sha256/275973a4ab66177e4d9c3d8b754e0c09739fd6815e28e9d40392fb3134c8f0f3"));

        assertThat(versionStatements.size(), Is.is(4));
        assertThat(versionStatements.get(0).getSubject(), Is.is(RefNodeFactory.toIRI("hash://sha256/0cfebefb2c2de0de893ab11071dcbaac2b75c217a566a1f3739577b9b12265e8")));
        assertThat(versionStatements.get(0).getPredicate(), Is.is(RefNodeConstants.WAS_DERIVED_FROM));
        assertThat(versionStatements.get(0).getObject(), Is.is(RefNodeFactory.toIRI("hash://sha256/19e965c900468b56f226bd7685dedb18940e07a1c044f166e6a602083583eb56")));
        assertThat(versionStatements.get(0).getGraphName().isPresent(), Is.is(false));

        assertThat(versionStatements.get(1).getSubject(), Is.is(RefNodeFactory.toIRI("hash://sha256/0cfebefb2c2de0de893ab11071dcbaac2b75c217a566a1f3739577b9b12265e8")));
        assertThat(versionStatements.get(1).getPredicate(), Is.is(RefNodeConstants.WAS_DERIVED_FROM));
        assertThat(versionStatements.get(1).getObject(), Is.is(RefNodeFactory.toIRI("hash://sha256/275973a4ab66177e4d9c3d8b754e0c09739fd6815e28e9d40392fb3134c8f0f3")));
        assertThat(versionStatements.get(1).getGraphName().isPresent(), Is.is(false));

        assertThat(versionStatements.get(2).getSubject(), Is.is(RefNodeFactory.toIRI("hash://sha256/19e965c900468b56f226bd7685dedb18940e07a1c044f166e6a602083583eb56")));
        assertThat(versionStatements.get(2).getPredicate(), Is.is(RefNodeConstants.WAS_DERIVED_FROM));
        assertThat(versionStatements.get(2).getObject(), Is.is(RefNodeFactory.toIRI("hash://sha256/431bef77364c1fda0740d18b4161ed40d1390b758b967eae1fd980971e1fac20")));
        assertThat(versionStatements.get(2).getGraphName().isPresent(), Is.is(false));

        assertRootOrigin(versionStatements.get(3), "hash://sha256/431bef77364c1fda0740d18b4161ed40d1390b758b967eae1fd980971e1fac20");
    }

    @Test
    public void tracingMergedOriginsWithRemote() throws IOException, URISyntaxException {
        KeyValueStore graphB = getKeyValueStoreForGraph("b/");
        KeyValueStore graphA = getKeyValueStoreForGraph("a/");

        assertNotNull(graphB.get(RefNodeFactory.toIRI("hash://sha256/0cfebefb2c2de0de893ab11071dcbaac2b75c217a566a1f3739577b9b12265e8")));

        List<String> notFound = new ArrayList<>();

        KeyValueStoreReadOnly notFoundCounter = new KeyValueStoreReadOnly() {
            @Override
            public InputStream get(IRI uri) throws IOException {
                notFound.add(uri.getIRIString());
                return null;
            }
        };
        KeyValueStore store = new KeyValueStoreWithFallback(graphB, graphA);
        KeyValueStoreReadOnly storeWithRemote = new KeyValueStoreWithFallback(store, notFoundCounter);

        List<Quad> versionStatements = new ArrayList<>();
        new ProvenanceTracerImpl(storeWithRemote, new ProcessorStateAlwaysContinue())
                .trace(RefNodeFactory.toIRI("hash://sha256/0cfebefb2c2de0de893ab11071dcbaac2b75c217a566a1f3739577b9b12265e8"),
                        new StatementListener() {
                            @Override
                            public void on(Quad statement) {
                                versionStatements.add(statement);
                            }
                        });

        assertThat(notFound.size(), Is.is(0));

        assertThat(versionStatements.get(0).getSubject(), Is.is(RefNodeFactory.toIRI("hash://sha256/0cfebefb2c2de0de893ab11071dcbaac2b75c217a566a1f3739577b9b12265e8")));
        assertThat(versionStatements.get(0).getPredicate(), Is.is(RefNodeConstants.WAS_DERIVED_FROM));
        assertThat(versionStatements.get(0).getObject(), Is.is(RefNodeFactory.toIRI("hash://sha256/19e965c900468b56f226bd7685dedb18940e07a1c044f166e6a602083583eb56")));
        assertThat(versionStatements.get(0).getGraphName().isPresent(), Is.is(false));

        assertThat(versionStatements.get(1).getSubject(), Is.is(RefNodeFactory.toIRI("hash://sha256/0cfebefb2c2de0de893ab11071dcbaac2b75c217a566a1f3739577b9b12265e8")));
        assertThat(versionStatements.get(1).getPredicate(), Is.is(RefNodeConstants.WAS_DERIVED_FROM));
        assertThat(versionStatements.get(1).getObject(), Is.is(RefNodeFactory.toIRI("hash://sha256/275973a4ab66177e4d9c3d8b754e0c09739fd6815e28e9d40392fb3134c8f0f3")));
        assertThat(versionStatements.get(1).getGraphName().isPresent(), Is.is(false));

        assertThat(versionStatements.get(2).getSubject(), Is.is(RefNodeFactory.toIRI("hash://sha256/19e965c900468b56f226bd7685dedb18940e07a1c044f166e6a602083583eb56")));
        assertThat(versionStatements.get(2).getPredicate(), Is.is(RefNodeConstants.WAS_DERIVED_FROM));
        assertThat(versionStatements.get(2).getObject(), Is.is(RefNodeFactory.toIRI("hash://sha256/431bef77364c1fda0740d18b4161ed40d1390b758b967eae1fd980971e1fac20")));
        assertThat(versionStatements.get(2).getGraphName().isPresent(), Is.is(false));

        assertThat(versionStatements.get(3).getSubject(), Is.is(RefNodeFactory.toIRI("hash://sha256/275973a4ab66177e4d9c3d8b754e0c09739fd6815e28e9d40392fb3134c8f0f3")));
        assertThat(versionStatements.get(3).getPredicate(), Is.is(RefNodeConstants.WAS_DERIVED_FROM));
        assertThat(versionStatements.get(3).getObject(), Is.is(RefNodeFactory.toIRI("hash://sha256/685bba795be181b6643652387e0d9907ae282200e52f6357b49429d99db073fd")));
        assertThat(versionStatements.get(3).getGraphName().isPresent(), Is.is(false));

        assertRootOrigin(versionStatements.get(4), "hash://sha256/431bef77364c1fda0740d18b4161ed40d1390b758b967eae1fd980971e1fac20");

        assertRootOrigin(versionStatements.get(5), "hash://sha256/685bba795be181b6643652387e0d9907ae282200e52f6357b49429d99db073fd");

        assertThat(versionStatements.size(), Is.is(6));

    }

    private KeyValueStore getKeyValueStoreForGraph(String graphName) throws URISyntaxException {
        String prefix = "/bio/guoda/preston/store/merged-a-into-b/";
        String suffix = "2a/5d/2a5de79372318317a382ea9a2cef069780b852b01210ef59e06b640a3539cb5a";
        URL resource = getClass().getResource(prefix + graphName + suffix);
        File file = new File(resource.toURI());
        File dataDir = file.getParentFile().getParentFile().getParentFile();

        return new KeyValueStoreLocalFileSystem(tmpDir.getRoot(),
                new KeyTo3LevelPath(dataDir.toURI()),
                new ValidatingKeyValueStreamHashTypeIRIFactory());
    }

    @Test
    public void nonRDF() throws IOException {

        List<String> requested = new ArrayList<>();

        KeyValueStoreReadOnly blobStore = new KeyValueStoreReadOnly() {
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
        new ProvenanceTracerImpl(blobStore, new ProcessorStateAlwaysContinue())
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