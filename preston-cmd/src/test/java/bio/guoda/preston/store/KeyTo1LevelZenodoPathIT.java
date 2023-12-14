package bio.guoda.preston.store;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.ResourcesHTTP;
import org.junit.Test;

import java.net.URI;

import static junit.framework.TestCase.assertNull;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNot.not;

public class KeyTo1LevelZenodoPathIT {


    @Test
    public void findFirstHit() {
        KeyTo1LevelZenodoPath keyTo1LevelZenodoPath = new KeyTo1LevelZenodoPath(URI.create("https://zenodo.org"), ResourcesHTTP::asInputStream);
        URI uri = keyTo1LevelZenodoPath.toPath(RefNodeFactory.toIRI("hash://md5/eb5e8f37583644943b86d1d9ebd4ded5"));
        assertThat(uri.toString(), not(is("https://zenodo.org/api/records/?q=_files.checksum:/eb5e8f37583644943b86d1d9ebd4ded5")));
    }

    @Test
    public void findFirstHitNonExisting() {
        KeyTo1LevelZenodoPath keyTo1LevelZenodoPath = new KeyTo1LevelZenodoPath(URI.create("https://zenodo.org"), ResourcesHTTP::asInputStream);
        assertNull(keyTo1LevelZenodoPath.toPath(RefNodeFactory.toIRI("hash://md5/d982d38c1b4dda6d3c1372a6c3e5d97e")));
    }

    @Test
    public void findFirstHitNewAPI() {
        KeyTo1LevelZenodoPath keyTo1LevelZenodoPath
                = new KeyTo1LevelZenodoPath(
                URI.create("https://zenodo.org"),
                ResourcesHTTP::asInputStream,
                KeyTo1LevelZenodoPath.ZENODO_API_PREFIX_2023_10_13,
                KeyTo1LevelZenodoPath.ZENODO_API_SUFFIX_2023_10_13
        );
        URI uri = keyTo1LevelZenodoPath.toPath(RefNodeFactory.toIRI("hash://md5/eb5e8f37583644943b86d1d9ebd4ded5"));
        assertThat(uri.toString(), is("https://zenodo.org/api/records/4589980/files/figure.png/content"));
    }


}