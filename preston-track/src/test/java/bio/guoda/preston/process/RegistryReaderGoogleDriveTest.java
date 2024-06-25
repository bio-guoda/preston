package bio.guoda.preston.process;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.KeyValueStoreReadOnly;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static bio.guoda.preston.process.RegistryReaderGoogleDrive.Type.docx;
import static bio.guoda.preston.process.RegistryReaderGoogleDrive.Type.pdf;
import static bio.guoda.preston.process.RegistryReaderGoogleDrive.Type.png;
import static org.hamcrest.MatcherAssert.assertThat;

public class RegistryReaderGoogleDriveTest {

    @Test
    public void handleVersionForGoogleResource() {
        assertExports("https://docs.google.com/document/d/1jzb54GbAkB_TOFIjWof5BW6gn1ujSnXXJQu6aZgFAB4/edit#heading=h.s91o7eupvwg7");
    }

    @Test
    public void handleVersionForGoogleResourceBareUrl() {
        assertExports("https://docs.google.com/document/d/1jzb54GbAkB_TOFIjWof5BW6gn1ujSnXXJQu6aZgFAB4");
    }

    @Test
    public void handleVersionForGoogleResourceBareUrlSpreadsheet() {
        List<Quad> statements = new ArrayList<>();
        RegistryReaderGoogleDrive registryReaderGoogleDrive = new RegistryReaderGoogleDrive(new KeyValueStoreReadOnly() {
            @Override
            public InputStream get(IRI uri) throws IOException {
                throw new IOException("kaboom!");
            }
        }, new StatementsListenerAdapter() {

            @Override
            public void on(Quad statement) {
                statements.add(statement);
            }
        });

        registryReaderGoogleDrive.on(
                RefNodeFactory.toStatement(
                        RefNodeFactory.toIRI("https://docs.google.com/spreadsheets/d/1wFuJ4RRlNirnrPfuY_d57I9_pnaNibw4nltNTkruSp0/edit?gid=1784126572#gid=1784126572"),
                        RefNodeConstants.HAS_VERSION,
                        RefNodeFactory.toIRI("foo:bar"))
        );

        assertThat(statements.size(), Is.is(18));

        Quad quad = statements.get(statements.size() - 1);
        assertThat(quad.getSubject(), Is.is(RefNodeFactory.toIRI("https://docs.google.com/spreadsheets/u/0/export?id=1wFuJ4RRlNirnrPfuY_d57I9_pnaNibw4nltNTkruSp0&format=zip")));
    }

    private void assertExports(String urlString) {
        List<Quad> statements = new ArrayList<>();
        RegistryReaderGoogleDrive registryReaderGoogleDrive = new RegistryReaderGoogleDrive(new KeyValueStoreReadOnly() {
            @Override
            public InputStream get(IRI uri) throws IOException {
                throw new IOException("kaboom!");
            }
        }, new StatementsListenerAdapter() {

            @Override
            public void on(Quad statement) {
                statements.add(statement);
            }
        });

        registryReaderGoogleDrive.on(
                RefNodeFactory.toStatement(
                        RefNodeFactory.toIRI(urlString),
                        RefNodeConstants.HAS_VERSION,
                        RefNodeFactory.toIRI("foo:bar"))
        );

        assertThat(statements.size(), Is.is(21));

        Quad quad = statements.get(statements.size() - 1);
        assertThat(quad.getSubject(), Is.is(RefNodeFactory.toIRI("https://docs.google.com/document/u/0/export?id=1jzb54GbAkB_TOFIjWof5BW6gn1ujSnXXJQu6aZgFAB4&format=html")));
    }

    @Test
    public void handleVersionForNonGoogleResource() {
        List<Quad> statements = new ArrayList<>();
        RegistryReaderGoogleDrive registryReaderGoogleDrive = new RegistryReaderGoogleDrive(new KeyValueStoreReadOnly() {
            @Override
            public InputStream get(IRI uri) throws IOException {
                throw new IOException("kaboom!");
            }
        }, new StatementsListenerAdapter() {

            @Override
            public void on(Quad statement) {
                statements.add(statement);
            }
        });

        registryReaderGoogleDrive.on(
                RefNodeFactory.toStatement(
                        RefNodeFactory.toIRI("https://example.org"),
                        RefNodeConstants.HAS_VERSION,
                        RefNodeFactory.toIRI("foo:bar"))
        );

        assertThat(statements.size(), Is.is(0));
    }

    @Test
    public void handleVersionForGoogleResourceExport() {
        List<Quad> statements = new ArrayList<>();
        RegistryReaderGoogleDrive registryReaderGoogleDrive = new RegistryReaderGoogleDrive(new KeyValueStoreReadOnly() {
            @Override
            public InputStream get(IRI uri) throws IOException {
                throw new IOException("kaboom!");
            }
        }, new StatementsListenerAdapter() {

            @Override
            public void on(Quad statement) {
                statements.add(statement);
            }
        });

        registryReaderGoogleDrive.on(
                RefNodeFactory.toStatement(
                        RefNodeFactory.toIRI("https://docs.google.com/document/u/0/export?id=1jzb54GbAkB_TOFIjWof5BW6gn1ujSnXXJQu6aZgFAB4&type=pdf"),
                        RefNodeConstants.HAS_VERSION,
                        RefNodeFactory.toIRI("foo:bar"))
        );

        assertThat(statements.size(), Is.is(0));
    }


    @Test
    public void detectDocURL() {
        String url = "https://docs.google.com/document/d/1jzb54GbAkB_TOFIjWof5BW6gn1ujSnXXJQu6aZgFAB4/edit#heading=h.s91o7eupvwg7";

        RegistryReaderGoogleDrive.GoogleResourceId gid = RegistryReaderGoogleDrive.getGoogleResourceId(url);

        assertThat(gid.getId(), Is.is("1jzb54GbAkB_TOFIjWof5BW6gn1ujSnXXJQu6aZgFAB4"));
        assertThat(gid.getType(), Is.is("document"));
    }

    @Test
    public void generateDerivedResourcesStatements() {
        String url = "https://docs.google.com/document/d/1jzb54GbAkB_TOFIjWof5BW6gn1ujSnXXJQu6aZgFAB4/edit#heading=h.s91o7eupvwg7";

        List<Quad> statements = new ArrayList<>();

        StatementEmitter emitter = new StatementEmitter() {

            @Override
            public void emit(Quad statement) {
                statements.add(statement);
            }
        };


        RegistryReaderGoogleDrive.emitExportStatements(RegistryReaderGoogleDrive.getGoogleResourceId(url), pdf, url, emitter);
        RegistryReaderGoogleDrive.emitExportStatements(RegistryReaderGoogleDrive.getGoogleResourceId(url), docx, url, emitter);


        assertThat(statements.size(), Is.is(6));

    }

    @Test
    public void generateDerivedResourcesStatements2() {
        String url = "https://docs.google.com/document/u/0/export?id=1jzb54GbAkB_TOFIjWof5BW6gn1ujSnXXJQu6aZgFAB4&type=pdf";

        List<Quad> statements = new ArrayList<>();

        StatementEmitter emitter = new StatementEmitter() {

            @Override
            public void emit(Quad statement) {
                statements.add(statement);
            }
        };

        RegistryReaderGoogleDrive.GoogleResourceId googleResourceId = RegistryReaderGoogleDrive.getGoogleResourceId(url);

        RegistryReaderGoogleDrive.emitExportStatements(googleResourceId, pdf, url, emitter);


        assertThat(statements.size(), Is.is(0));

    }


    @Test
    public void detectSheetsURL() {
        String url = "https://docs.google.com/spreadsheets/d/1wFuJ4RRlNirnrPfuY_d57I9_pnaNibw4nltNTkruSp0/edit#gid=1784126572";

        RegistryReaderGoogleDrive.GoogleResourceId googleResourceId = RegistryReaderGoogleDrive.getGoogleResourceId(url);

        assertThat(googleResourceId.getId(), Is.is("1wFuJ4RRlNirnrPfuY_d57I9_pnaNibw4nltNTkruSp0"));
        assertThat(googleResourceId.getType(), Is.is("spreadsheets"));
        assertThat(googleResourceId.getGid(), Is.is("1784126572"));
        assertThat(RegistryReaderGoogleDrive.toExportIRI(googleResourceId, RegistryReaderGoogleDrive.Type.tsv).getIRIString(),
                Is.is("https://docs.google.com/spreadsheets/u/0/export?id=1wFuJ4RRlNirnrPfuY_d57I9_pnaNibw4nltNTkruSp0&gid=1784126572&format=tsv"));
    }

    @Test
    public void detectPresentationSheetsURL() {
        String url = "https://docs.google.com/presentation/d/1kV0tVscrYO6WxZYRupnzjVQecmmky27Oc7YeePZjBXg/edit#slide=id.p";

        RegistryReaderGoogleDrive.GoogleResourceId googleResourceId = RegistryReaderGoogleDrive.getGoogleResourceId(url);
        assertThat(googleResourceId.getId(), Is.is("1kV0tVscrYO6WxZYRupnzjVQecmmky27Oc7YeePZjBXg"));
        assertThat(googleResourceId.getType(), Is.is("presentation"));
        assertThat(googleResourceId.getGid(), Is.is("p"));


    }

    @Test
    public void detectPresentationSheetsURLToPng() {
        String url = "https://docs.google.com/presentation/d/1VknOvxzuUVPKMOlo8wj7w9NOmyHf6P7YpN6eF1dMrlk/edit#slide=id.p";

        RegistryReaderGoogleDrive.GoogleResourceId googleResourceId = RegistryReaderGoogleDrive.getGoogleResourceId(url);
        assertThat(googleResourceId.getId(), Is.is("1VknOvxzuUVPKMOlo8wj7w9NOmyHf6P7YpN6eF1dMrlk"));
        assertThat(googleResourceId.getType(), Is.is("presentation"));
        assertThat(googleResourceId.getGid(), Is.is("p"));


    }

    @Test
    public void generateExportUrl() {
        RegistryReaderGoogleDrive.GoogleResourceId gid
                = new RegistryReaderGoogleDrive.GoogleResourceId(
                "1kV0tVscrYO6WxZYRupnzjVQecmmky27Oc7YeePZjBXg",
                "presentation",
                null
        );

        IRI iri = RegistryReaderGoogleDrive.toExportIRI(gid, png);

        assertThat(iri.getIRIString(), Is.is("https://docs.google.com/presentation/u/0/export?id=1kV0tVscrYO6WxZYRupnzjVQecmmky27Oc7YeePZjBXg&format=png"));
    }

    @Test
    public void generateExportUrlSpecificSheet() {
        RegistryReaderGoogleDrive.GoogleResourceId gid
                = new RegistryReaderGoogleDrive.GoogleResourceId(
                "1VknOvxzuUVPKMOlo8wj7w9NOmyHf6P7YpN6eF1dMrlk",
                "presentation",
                "g274abf48da0_0_10"
        );

        IRI iri = RegistryReaderGoogleDrive.toExportIRI(gid, png);

        assertThat(iri.getIRIString(), Is.is("https://docs.google.com/presentation/u/0/export?id=1VknOvxzuUVPKMOlo8wj7w9NOmyHf6P7YpN6eF1dMrlk&pageid=g274abf48da0_0_10&format=png"));

    }

    @Test
    public void detectPresentationSheetsURL2() {
        String url = "https://docs.google.com/presentation/d/1VknOvxzuUVPKMOlo8wj7w9NOmyHf6P7YpN6eF1dMrlk/edit#slide=id.g274abf48da0_0_10";
        RegistryReaderGoogleDrive.GoogleResourceId googleResourceId = RegistryReaderGoogleDrive.getGoogleResourceId(url);
        assertThat(googleResourceId.getId(), Is.is("1VknOvxzuUVPKMOlo8wj7w9NOmyHf6P7YpN6eF1dMrlk"));
        assertThat(googleResourceId.getType(), Is.is("presentation"));
        assertThat(RegistryReaderGoogleDrive.toExportIRI(googleResourceId, RegistryReaderGoogleDrive.Type.txt).getIRIString(),
                Is.is("https://docs.google.com/presentation/u/0/export?id=1VknOvxzuUVPKMOlo8wj7w9NOmyHf6P7YpN6eF1dMrlk&format=txt"));
    }

}