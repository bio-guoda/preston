package bio.guoda.preston.store;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.ResourcesHTTP;
import org.junit.Ignore;
import org.junit.Test;

import java.net.URI;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNot.not;

public class KeyTo1LevelZenodoPathIT {


    @Ignore("after 2023-10-13 migration of Zenodo infrastructure this integration test stopped working; see https://github.com/bio-guoda/preston/issues/266")
    @Test
    public void findFirstHit() {
        KeyTo1LevelZenodoPath keyTo1LevelZenodoPath = new KeyTo1LevelZenodoPath(URI.create("https://zenodo.org"), ResourcesHTTP::asInputStream);
        URI uri = keyTo1LevelZenodoPath.toPath(RefNodeFactory.toIRI("hash://md5/eb5e8f37583644943b86d1d9ebd4ded5"));
        assertThat(uri.toString(), not(is("https://zenodo.org/api/records/?q=_files.checksum:/eb5e8f37583644943b86d1d9ebd4ded5")));
    }

    @Test
    public void findFirstHitNewAPI() {
        KeyTo1LevelZenodoPath keyTo1LevelZenodoPath
                = new KeyTo1LevelZenodoPath(
                URI.create("https://zenodo.org"),
                ResourcesHTTP::asInputStream,
                "https://zenodo.org/api/records?q=files.entries.checksum:",
                "%22&allversions=1");
        URI uri = keyTo1LevelZenodoPath.toPath(RefNodeFactory.toIRI("hash://md5/eb5e8f37583644943b86d1d9ebd4ded5"));
        assertThat(uri.toString(), is("https://zenodo.org/api/records/4589980/files/figure.png/content"));
    }


}