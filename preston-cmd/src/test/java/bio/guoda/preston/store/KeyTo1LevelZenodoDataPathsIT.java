package bio.guoda.preston.store;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.ResourcesHTTP;
import org.junit.Test;

import java.net.URI;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class KeyTo1LevelZenodoDataPathsIT {


    @Test
    public void findFirstHit() {
        KeyToPath dataContentId = new KeyTo1LevelZenodoDataPaths(URI.create("https://zenodo.org"), ResourcesHTTP::asInputStream);
        URI uri = dataContentId.toPath(RefNodeFactory.toIRI("hash://md5/2d9ef974add28bfe8f02b868736b147a"));
        assertThat(uri.toString(), is("hash://md5/b871e22f0e8c576305f99cb5aff8cddd"));
    }

    @Test
    public void findFirstHit2023() {
        KeyToPath dataContentId = new KeyTo1LevelZenodoDataPaths(
                URI.create("https://zenodo.org"),
                ResourcesHTTP::asInputStream,
                KeyTo1LevelZenodoDataPaths.ZENODO_API_PREFIX_2023_10_13,
                KeyTo1LevelZenodoDataPaths.ZENODO_API_SUFFIX_2023_10_13
        );
        URI uri = dataContentId.toPath(RefNodeFactory.toIRI("hash://md5/2d9ef974add28bfe8f02b868736b147a"));
        assertThat(uri.toString(), is("hash://md5/b871e22f0e8c576305f99cb5aff8cddd"));
    }

}