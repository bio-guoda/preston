package bio.guoda.preston.store;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.ResourcesHTTP;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.Test;

import java.net.URI;

import static junit.framework.TestCase.assertNull;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.number.OrderingComparison.greaterThan;

public class KeyTo1LevelDataVersePathIT {

    @Test
    public void harvardDataVerse() {
        KeyTo1LevelDataVersePath keyToPath
                = new KeyTo1LevelDataVersePath(
                URI.create("https://dataverse.harvard.edu"),
                ResourcesHTTP::asInputStream
        );
        URI uri = keyToPath.toPath(RefNodeFactory.toIRI("hash://md5/48a76222cf5c06cb4f2d8f75cc0caa63"));
        assertThat(uri.toString(), is("https://dataverse.harvard.edu/api/access/datafile/2829688"));
    }

    @Test
    public void magicHost() {
        KeyTo1LevelDataVersePath keyToPath
                = new KeyTo1LevelDataVersePath(
                URI.create("https://dataverse.org"),
                ResourcesHTTP::asInputStream
        );
        StopWatch watch = new StopWatch();
        watch.start();
        URI uri = keyToPath.toPath(RefNodeFactory.toIRI("hash://md5/48a76222cf5c06cb4f2d8f75cc0caa63"));
        assertThat(uri.toString(), is("https://dataverse.harvard.edu/api/access/datafile/2829688"));
        watch.stop();

        StopWatch watchSecond = new StopWatch();
        watchSecond.start();
        uri = keyToPath.toPath(RefNodeFactory.toIRI("hash://md5/48a76222cf5c06cb4f2d8f75cc0caa63"));
        assertThat(uri.toString(), is("https://dataverse.harvard.edu/api/access/datafile/2829688"));
        watchSecond.stop();

        assertThat(watch.getTime(), greaterThan(watchSecond.getTime()));

    }

    @Test
    public void unlikelyDataVerseHost() {
        KeyTo1LevelDataVersePath keyToPath
                = new KeyTo1LevelDataVersePath(
                URI.create("https://example.org"),
                ResourcesHTTP::asInputStream
        );
        URI uri = keyToPath.toPath(RefNodeFactory.toIRI("hash://md5/48a76222cf5c06cb4f2d8f75cc0caa63"));
        assertNull(uri);
    }


}