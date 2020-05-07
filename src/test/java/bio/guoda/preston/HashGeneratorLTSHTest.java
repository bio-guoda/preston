package bio.guoda.preston;

import com.trendmicro.tlsh.Tlsh;
import org.apache.commons.rdf.api.IRI;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class HashGeneratorLTSHTest {

    @Test
    public void calculateAndDiffHashSimilarStreaming() throws IOException {
        assertHashOutcomes(new HashGeneratorLTSH());
    }

    @Test
    public void calculateAndDiffHashNotSimilarStreaming() throws IOException {
        HashGenerator hasher = new HashGeneratorLTSH();
        assertNotSimilarHashes(hasher);
    }

    private void assertNotSimilarHashes(HashGenerator<String> hasher) throws IOException {
        String complete = "/bio/guoda/preston/process/idigbio-recordsets-complete.json";
        String hash = hasher.hash(getClass().getResourceAsStream(complete));

        IRI iri = bio.guoda.preston.Hasher.calcSHA256(getClass().getResourceAsStream(complete));
        assertThat(iri.getIRIString(), is("hash://sha256/0931a831be557bfedd27b0068d9a0a9f14c1b92cbf9199e8ba79e04d0a6baedc"));
        assertThat(hash, is("1ac4d824c9a50ea305c621a9bdd94583e25052972e447c047f4c8b5c4feee2fbafa3dd"));

        String incomplete1 = "/bio/guoda/preston/process/intermountain-biota-rss.xml";
        IRI iri2 = bio.guoda.preston.Hasher.calcSHA256(getClass().getResourceAsStream(incomplete1));
        assertThat(iri2.getIRIString(), is("hash://sha256/ab98ab8bf799d0fb0556d1502defda39fb974edc70b9a4f144bc2af33142b718"));

        String otherHash = hasher.hash(getClass().getResourceAsStream(incomplete1));
        assertThat(otherHash, is("ce127c9a35ff967802c1d98039d0607dfea083bb9dd441a0fc9d52ea9b867cb91e7305"));

        Tlsh h1 = Tlsh.fromTlshStr(hash);
        Tlsh h2 = Tlsh.fromTlshStr(otherHash);

        int diff = h1.totalDiff(h2, true);
        assertThat(diff, is(795));
    }

    private void assertHashOutcomes(HashGenerator<String> hasher) throws IOException {
        String complete = "/bio/guoda/preston/process/idigbio-recordsets-complete.json";

        String hash = hasher.hash(getClass().getResourceAsStream(complete));


        IRI iri = bio.guoda.preston.Hasher.calcSHA256(getClass().getResourceAsStream(complete));
        assertThat(iri.getIRIString(), is("hash://sha256/0931a831be557bfedd27b0068d9a0a9f14c1b92cbf9199e8ba79e04d0a6baedc"));
        assertThat(hash, is("1ac4d824c9a50ea305c621a9bdd94583e25052972e447c047f4c8b5c4feee2fbafa3dd"));


        String incomplete1 = "/bio/guoda/preston/process/idigbio-recordsets-incomplete.json";
        IRI iri2 = bio.guoda.preston.Hasher.calcSHA256(getClass().getResourceAsStream(incomplete1));
        assertThat(iri2.getIRIString(), is("hash://sha256/6985609de9dd5487e181a635da5ef04601f0478196929a85d105f1a838e9e1f6"));

        String otherHash = hasher.hash(getClass().getResourceAsStream(incomplete1));
        assertThat(otherHash, is("f7c4d824c9a50ea305c621a9bdd94583e25052972e447c047f4c8b5c4feee2fbafa3dd"));

        Tlsh h1 = Tlsh.fromTlshStr(hash);
        Tlsh h2 = Tlsh.fromTlshStr(otherHash);

        int diff = h1.totalDiff(h2, true);
        assertThat(diff, is(1));
    }

}
