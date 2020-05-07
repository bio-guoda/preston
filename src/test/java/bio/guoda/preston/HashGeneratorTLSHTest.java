package bio.guoda.preston;

import bio.guoda.preston.store.TestUtil;
import com.trendmicro.tlsh.Tlsh;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class HashGeneratorTLSHTest {

    @Test
    public void calculateAndDiffHashSimilarStreaming() throws IOException {
        assertHashSimilar(new HashGeneratorTLSH());
    }

    @Test
    public void calculateAndDiffHashNotSimilarStreaming() throws IOException {
        HashGenerator<String> hasher = new HashGeneratorTLSH();
        assertNotSimilarHashes(hasher);
    }

    private void assertNotSimilarHashes(HashGenerator<String> hasher) throws IOException {
        String complete = "/bio/guoda/preston/process/idigbio-recordsets-complete.json";

        InputStream resourceAsStream = getClass().getResourceAsStream(complete);
        String hash = hasher.hash(TestUtil.filterLineFeedFromTextInputStream(resourceAsStream));
        assertThat(hash, is("1ac4d824c9a50ea305c621a9bdd94583e25052972e447c047f4c8b5c4feee2fbafa3dd"));

        String incomplete1 = "/bio/guoda/preston/process/intermountain-biota-rss.xml";

        String otherHash = hasher.hash(TestUtil.filterLineFeedFromTextInputStream(getClass().getResourceAsStream(incomplete1)));
        assertThat(otherHash, is("ce127c9a35ff967802c1d98039d0607dfea083bb9dd441a0fc9d52ea9b867cb91e7305"));

        Tlsh h1 = Tlsh.fromTlshStr(hash);
        Tlsh h2 = Tlsh.fromTlshStr(otherHash);

        int diff = h1.totalDiff(h2, true);
        assertThat(diff, is(795));
    }

    private void assertHashSimilar(HashGenerator<String> hasher) throws IOException {
        String complete = "/bio/guoda/preston/process/idigbio-recordsets-complete.json";

        String hash = hasher.hash(TestUtil.filterLineFeedFromTextInputStream(getClass().getResourceAsStream(complete)));
        assertThat(hash, is("1ac4d824c9a50ea305c621a9bdd94583e25052972e447c047f4c8b5c4feee2fbafa3dd"));

        String incomplete1 = "/bio/guoda/preston/process/idigbio-recordsets-incomplete.json";

        String otherHash = hasher.hash(TestUtil.filterLineFeedFromTextInputStream(getClass().getResourceAsStream(incomplete1)));
        assertThat(otherHash, is("f7c4d824c9a50ea305c621a9bdd94583e25052972e447c047f4c8b5c4feee2fbafa3dd"));

        Tlsh h1 = Tlsh.fromTlshStr(hash);
        Tlsh h2 = Tlsh.fromTlshStr(otherHash);

        int diff = h1.totalDiff(h2, true);
        assertThat(diff, is(1));
    }

}
