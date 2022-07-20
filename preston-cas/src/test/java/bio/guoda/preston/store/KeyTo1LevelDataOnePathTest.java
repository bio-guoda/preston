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

public class KeyTo1LevelDataOnePathTest {

    @Test
    public void parseResponse() throws IOException, URISyntaxException {


        InputStream is = getClass().getResourceAsStream("dataone-response.json");
        List<URI> candidates = KeyTo1LevelDataOnePath.parseResponse(is);

        assertThat(candidates.size(), Is.is(4));

        assertThat(candidates.get(0).toString(), Is.is("https://cn.dataone.org/cn/v2/resolve/ess-dive-0462dff585f94f8-20180716T160600643874"));

    }

    @Test
    public void toPath() {
        IRI hash = RefNodeFactory.toIRI("hash://md5/e27c99a7f701dab97b7d09c467acf468");

        URI actualPath = new KeyTo1LevelDataOnePath(URI.create("https://dataone.org"), getDeref())
                .toPath(hash);
        assertThat(actualPath.toString(),
                Is.is("https://mn-orc-1.dataone.org/knb/d1/mn/v2/object/ess-dive-0462dff585f94f8-20180716T160600643874"));
    }

    Dereferencer<InputStream> getDeref() {
        return new Dereferencer<InputStream>() {

            @Override
            public InputStream get(IRI uri) throws IOException {
                if (StringUtils.equals(uri.getIRIString(), "https://cn.dataone.org/cn/v2/query/solr/?q=checksum:e27c99a7f701dab97b7d09c467acf468&fl=identifier,size,formatId,checksum,checksumAlgorithm,replicaMN,dataUrl&rows=10&wt=json")) {
                    return KeyTo1LevelDataOnePathTest.this.getClass().getResourceAsStream("dataone-response.json");
                } else if (StringUtils.equals(uri.getIRIString(), "https://cn.dataone.org/cn/v2/resolve/ess-dive-0462dff585f94f8-20180716T160600643874")) {
                    return KeyTo1LevelDataOnePathTest.this.getClass().getResourceAsStream("dataone-dataentries.xml");
                }
                throw new IOException("not supported" + uri.getIRIString());
            }
        };
    }

    @Test
    public void toPath2() {
        URI actualPath
                = new KeyTo1LevelDataOnePath(URI.create("https://dataone.org"), getDeref())
                .toPath(getMD5IRI());
        assertThat(actualPath.toString(),
                Is.is("https://mn-orc-1.dataone.org/knb/d1/mn/v2/object/ess-dive-0462dff585f94f8-20180716T160600643874"));
    }

    @Test
    public void nonDataOneNoTrailingSlash() {

        URI actualPath
                = new KeyTo1LevelDataOnePath(URI.create("https://deeplinker.bio"), getExplodingReref())
                .toPath(getMD5IRI());
        assertThat(actualPath.toString(),
                Is.is("https://deeplinker.bio/e27c99a7f701dab97b7d09c467acf468"));
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
    public void nonDataOneTrailingSlash() {

        URI actualPath = new KeyTo1LevelDataOnePath(URI.create("https://deeplinker.bio/"), getExplodingReref())
                .toPath(getMD5IRI());
        assertThat(actualPath.toString(),
                Is.is("https://deeplinker.bio/e27c99a7f701dab97b7d09c467acf468"));
    }

    private IRI getMD5IRI() {
        return RefNodeFactory.toIRI("hash://md5/e27c99a7f701dab97b7d09c467acf468");
    }


    @Test
    public void toTryPath() {
        URI actualPath = new KeyTo1LevelDataOnePath(URI.create("https://dataone.org/blabla/"), getExplodingReref())
                .toPath(getMD5IRI());
        assertThat(actualPath.toString(), Is.is("https://dataone.org/blabla/e27c99a7f701dab97b7d09c467acf468"));
    }

    @Test
    public void dataoneAPIDirect() {
        URI baseURI = URI.create(KeyTo1LevelDataOnePath.DATAONE_URL_PREFIX);
        URI actualPath = new KeyTo1LevelDataOnePath(baseURI, getDeref())
                .toPath(getMD5IRI());
        assertThat(actualPath.toString(),
                Is.is("https://mn-orc-1.dataone.org/knb/d1/mn/v2/object/ess-dive-0462dff585f94f8-20180716T160600643874"));
    }


}