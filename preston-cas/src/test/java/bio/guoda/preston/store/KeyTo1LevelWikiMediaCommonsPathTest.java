package bio.guoda.preston.store;

import bio.guoda.preston.RefNodeFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNull;

public class KeyTo1LevelWikiMediaCommonsPathTest {

    @Test
    public void parseResponse() throws IOException, URISyntaxException {
        InputStream is = getClass().getResourceAsStream("wikimedia-commons-response.json");
        List<URI> candidates = KeyTo1LevelWikiMediaCommonsPath.parseResponse(is);

        assertThat(candidates.size(), Is.is(1));

        assertThat(candidates.get(0).toString(), Is.is("https://upload.wikimedia.org/wikipedia/commons/c/ce/Johann_Adam_Klein_-_Cossacks_Eat_a_Meal_in_the_Field_%281819%29%2C_Thorvaldsens_Museum_E721%2C6.jpg"));

    }

    @Test
    public void toPath() {
        IRI hash = RefNodeFactory.toIRI("hash://sha1/b12eb7f6b43a3cc1eefc54db90ec5ae1504c3f17");

        URI actualPath = new KeyTo1LevelWikiMediaCommonsPath(URI.create("https://wikimedia.org"), getDeref("b12eb7f6b43a3cc1eefc54db90ec5ae1504c3f17"))
                .toPath(hash);
        assertThat(actualPath.toString(),
                Is.is("https://upload.wikimedia.org/wikipedia/commons/c/ce/Johann_Adam_Klein_-_Cossacks_Eat_a_Meal_in_the_Field_%281819%29%2C_Thorvaldsens_Museum_E721%2C6.jpg"));
    }

    @Test
    public void toPathError() {
        final String sha1Hash = "398ab74e3da160d52705bb2477eb0f2f2cde5f15";
        IRI hash = RefNodeFactory.toIRI("hash://sha1/" + sha1Hash);

        URI actualPath = new KeyTo1LevelWikiMediaCommonsPath(URI.create("https://wikimedia.org"),
                new Dereferencer<InputStream>() {

                    @Override
                    public InputStream get(IRI uri) throws IOException {
                        if (StringUtils.equals(uri.getIRIString(), "https://commons.wikimedia.org/w/api.php?action=query&list=allimages&format=json&aisha1=" + sha1Hash)) {
                            return KeyTo1LevelWikiMediaCommonsPathTest.this.getClass().getResourceAsStream("wikimedia-commons-response-no-hits.json");
                        }
                        throw new IOException("not supported [" + uri.getIRIString() + "]");
                    }
                })
                .toPath(hash);

        assertNull(actualPath);
    }

    @Test
    public void toPath2() {
        IRI hash = RefNodeFactory.toIRI("hash://sha1/86fa30f32d9c557ea5d2a768e9c3595d3abb17a2");

        URI actualPath = new KeyTo1LevelWikiMediaCommonsPath(URI.create("https://wikimedia.org"), getDeref2())
                .toPath(hash);
        assertThat(actualPath.toString(),
                Is.is("https://upload.wikimedia.org/wikipedia/commons/1/1f/Oryctolagus_cuniculus_Rcdo.jpg"));
    }

    Dereferencer<InputStream> getDeref(final String sha1Hash) {
        return new Dereferencer<InputStream>() {

            @Override
            public InputStream get(IRI uri) throws IOException {
                if (StringUtils.equals(uri.getIRIString(), "https://commons.wikimedia.org/w/api.php?action=query&list=allimages&format=json&aisha1=" + sha1Hash)) {
                    return KeyTo1LevelWikiMediaCommonsPathTest.this.getClass().getResourceAsStream("wikimedia-commons-response.json");
                }
                throw new IOException("not supported [" + uri.getIRIString() + "]");
            }
        };
    }

    Dereferencer<InputStream> getDeref2() {
        return new Dereferencer<InputStream>() {

            @Override
            public InputStream get(IRI uri) throws IOException {
                if (StringUtils.equals(uri.getIRIString(), "https://commons.wikimedia.org/w/api.php?action=query&list=allimages&format=json&aisha1=86fa30f32d9c557ea5d2a768e9c3595d3abb17a2")) {
                    return KeyTo1LevelWikiMediaCommonsPathTest.this.getClass().getResourceAsStream("wikimedia-commons-response-bunny.json");
                }
                throw new IOException("not supported" + uri.getIRIString());
            }
        };
    }



}