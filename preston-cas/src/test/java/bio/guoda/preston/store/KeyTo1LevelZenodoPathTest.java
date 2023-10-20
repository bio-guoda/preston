package bio.guoda.preston.store;

import bio.guoda.preston.RefNodeFactory;
import org.apache.commons.rdf.api.IRI;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import static junit.framework.TestCase.assertNotNull;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class KeyTo1LevelZenodoPathTest {

    @Test
    public void toPath() {
        IRI hash = RefNodeFactory.toIRI("hash://md5/eb5e8f37583644943b86d1d9ebd4ded5");

        URI actualPath = new KeyTo1LevelZenodoPath(URI.create("https://zenodo.org"), getDeref())
                .toPath(hash);
        assertThat(actualPath.toString(), Is.is("https://zenodo.org/api/files/cbb44724-b635-4c75-94e8-c7a824efbc72/figure.png"));
    }

    Dereferencer<InputStream> getDeref() {
        return new Dereferencer<InputStream>() {

            @Override
            public InputStream get(IRI uri) throws IOException {
                assertThat(uri.getIRIString(), is("https://zenodo.org/api/records/?q=_files.checksum:%22md5:eb5e8f37583644943b86d1d9ebd4ded5%22&all_versions=true"));
                return KeyTo1LevelZenodoPathTest.this.getClass().getResourceAsStream("zenodo-response.json");
            }
        };
    }

    @Test
    public void toPath20231020() {
        IRI hash = RefNodeFactory.toIRI("hash://md5/eb5e8f37583644943b86d1d9ebd4ded5");

        Dereferencer<InputStream> dereferencer = new Dereferencer<InputStream>() {

            @Override
            public InputStream get(IRI uri) throws IOException {
                assertThat(uri.getIRIString(), is("https://zenodo.org/api/records?q=files.entries.checksum:%22md5:eb5e8f37583644943b86d1d9ebd4ded5%22&allversions=1"));
                return KeyTo1LevelZenodoPathTest.this.getClass().getResourceAsStream("zenodo-response-all-versions-2023-10-20.json");
            }
        };

        URI actualPath = new KeyTo1LevelZenodoPath(
                URI.create("https://zenodo.org"),
                dereferencer,
                "https://zenodo.org/api/records?q=files.entries.checksum:",
                "%22&allversions=1"
        )
                .toPath(hash);
        assertThat(actualPath.toString(), Is.is("https://zenodo.org/api/records/4589980/files/figure.png"));
    }


    @Test
    public void toPathOlderVersion() {
        IRI hash = RefNodeFactory.toIRI("hash://md5/d11ddcecf3d5cbc627439260bdbfda72");

        URI actualPath = new KeyTo1LevelZenodoPath(URI.create("https://zenodo.org"), getDerefREADMEOlder())
                .toPath(hash);
        assertThat(actualPath.toString(), Is.is("https://zenodo.org/api/files/b522e8c8-f898-4861-b4e2-453e52ec9f56/README"));
    }

    Dereferencer<InputStream> getDerefREADMEOlder() {
        return new Dereferencer<InputStream>() {

            @Override
            public InputStream get(IRI uri) throws IOException {
                assertThat(uri.getIRIString(), is("https://zenodo.org/api/records/?q=_files.checksum:%22md5:d11ddcecf3d5cbc627439260bdbfda72%22&all_versions=true"));
                return KeyTo1LevelZenodoPathTest.this.getClass().getResourceAsStream("zenodo-response-all-versions.json");
            }
        };
    }

    @Test
    public void toPath2() {
        URI actualPath
                = new KeyTo1LevelZenodoPath(URI.create("https://zenodo.org"), getDeref())
                .toPath(getMD5IRI());
        assertThat(actualPath.toString(), Is.is("https://zenodo.org/api/files/cbb44724-b635-4c75-94e8-c7a824efbc72/figure.png"));
    }

    @Test
    public void findFirstHit() throws IOException {
        InputStream is = getClass().getResourceAsStream("zenodo-response.json");
        URI bla = KeyTo1LevelZenodoPath.findFirstHit("eb5e8f37583644943b86d1d9ebd4ded5", is);
        assertNotNull(bla);
        assertThat(bla.toString(), is("https://zenodo.org/api/files/cbb44724-b635-4c75-94e8-c7a824efbc72/figure.png"));
    }

    @Test(expected = IOException.class)
    public void findFirstHitMalformed() throws IOException {
        InputStream is = getClass().getResourceAsStream("zenodo-response-malformed.json");
        KeyTo1LevelZenodoPath.findFirstHit("eb5e8f37583644943b86d1d9ebd4ded5", is);
    }

    @Test(expected = IOException.class)
    public void findFirstHitNull() throws IOException {
        KeyTo1LevelZenodoPath.findFirstHit("eb5e8f37583644943b86d1d9ebd4ded5", null);
    }

    @Test
    public void nonZenodoNoTrailingSlash() {

        URI actualPath
                = new KeyTo1LevelZenodoPath(URI.create("https://linker.bio"), getExplodingReref())
                .toPath(getMD5IRI());
        assertThat(actualPath.toString(), Is.is("https://linker.bio/eb5e8f37583644943b86d1d9ebd4ded5"));
    }

    private Dereferencer<InputStream> getExplodingReref() {
        return new Dereferencer<InputStream>() {
            @Override
            public InputStream get(IRI uri) throws IOException {
                throw new IOException("kaboom!");
            }
        };
    }

    @Test
    public void nonZenodoTrailingSlash() {

        URI actualPath = new KeyTo1LevelZenodoPath(URI.create("https://linker.bio/"), getExplodingReref())
                .toPath(getMD5IRI());
        assertThat(actualPath.toString(), Is.is("https://linker.bio/eb5e8f37583644943b86d1d9ebd4ded5"));
    }

    private IRI getMD5IRI() {
        return RefNodeFactory.toIRI("hash://md5/eb5e8f37583644943b86d1d9ebd4ded5");
    }


    @Test
    public void toTryPath() {
        URI actualPath = new KeyTo1LevelZenodoPath(URI.create("https://zenodo.org/blabla/"), getExplodingReref())
                .toPath(getMD5IRI());
        assertThat(actualPath.toString(), Is.is("https://zenodo.org/blabla/eb5e8f37583644943b86d1d9ebd4ded5"));
    }

    @Test
    public void zenodoAPIDirect() {
        URI baseURI = URI.create(KeyTo1LevelZenodoPath.ZENODO_API_PREFIX);
        URI actualPath = new KeyTo1LevelZenodoPath(baseURI, getDeref())
                .toPath(getMD5IRI());
        assertThat(actualPath.toString(), Is.is("https://zenodo.org/api/files/cbb44724-b635-4c75-94e8-c7a824efbc72/figure.png"));
    }

}