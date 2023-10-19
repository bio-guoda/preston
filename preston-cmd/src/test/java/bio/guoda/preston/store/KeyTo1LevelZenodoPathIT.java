package bio.guoda.preston.store;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.ResourcesHTTP;
import org.junit.Test;

import java.net.URI;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class KeyTo1LevelZenodoPathIT {


    @Test
    public void findFirstHit() {
        KeyTo1LevelZenodoPath keyTo1LevelZenodoPath = new KeyTo1LevelZenodoPath(URI.create("https://zenodo.org"), ResourcesHTTP::asInputStream);
        URI uri = keyTo1LevelZenodoPath.toPath(RefNodeFactory.toIRI("hash://md5/eb5e8f37583644943b86d1d9ebd4ded5"));
        assertThat(uri.toString(), is("https://zenodo.org/api/records/?q=_files.checksum:/eb5e8f37583644943b86d1d9ebd4ded5"));
    }


}