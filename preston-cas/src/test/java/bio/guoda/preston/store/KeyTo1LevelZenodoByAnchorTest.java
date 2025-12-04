package bio.guoda.preston.store;

import bio.guoda.preston.RefNodeFactory;
import org.apache.commons.rdf.api.IRI;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.net.URI;

import static org.hamcrest.MatcherAssert.assertThat;

public class KeyTo1LevelZenodoByAnchorTest {

    @Test
    public void findAnchoredDepositCandidateMD5AnchorMD5Key() {
        IRI anchor = RefNodeFactory.toIRI("hash://md5/2d9ef974add28bfe8f02b868736b147a");
        KeyTo1LevelZenodoByAnchor keyTo1LevelZenodoByAnchor = new KeyTo1LevelZenodoByAnchor(new KeyToPath() {
            @Override
            public URI toPath(IRI key) {
                return URI.create("hash://md5/b871e22f0e8c576305f99cb5aff8cddd");
            }

            @Override
            public boolean supports(IRI key) {
                return true;
            }
        }, anchor);

        IRI actual = RefNodeFactory.toIRI(keyTo1LevelZenodoByAnchor.toPath(RefNodeFactory.toIRI("hash://md5/f5de551a5a7354a8a4ced2dd31a2c4db")));
        IRI expected = RefNodeFactory.toIRI("zip:hash://md5/b871e22f0e8c576305f99cb5aff8cddd!/data/f5/de/f5de551a5a7354a8a4ced2dd31a2c4db");
        assertThat(
                actual.getIRIString(),
                Is.is(expected.getIRIString())
        );
    }

    @Test
    public void findAnchoredDepositCandidateMD5AnchorMD5KeyAPIContentURI() {
        IRI anchor = RefNodeFactory.toIRI("hash://md5/2d9ef974add28bfe8f02b868736b147a");
        KeyTo1LevelZenodoByAnchor keyTo1LevelZenodoByAnchor = new KeyTo1LevelZenodoByAnchor(new KeyToPath() {
            @Override
            public URI toPath(IRI key) {
                return URI.create("hash://md5/b871e22f0e8c576305f99cb5aff8cddd");
            }

            @Override
            public boolean supports(IRI key) {
                return true;
            }
        }, anchor);

        IRI actual = RefNodeFactory.toIRI(keyTo1LevelZenodoByAnchor.toPath(RefNodeFactory.toIRI("hash://md5/f5de551a5a7354a8a4ced2dd31a2c4db")));
        IRI expected = RefNodeFactory.toIRI("zip:hash://md5/b871e22f0e8c576305f99cb5aff8cddd!/data/f5/de/f5de551a5a7354a8a4ced2dd31a2c4db");
        assertThat(
                actual.getIRIString(),
                Is.is(expected.getIRIString())
        );
    }

    @Test
    public void findAnchoredDepositCandidateMD5AnchorSHA256Key() {
        IRI anchor = RefNodeFactory.toIRI("hash://md5/2d9ef974add28bfe8f02b868736b147a");
        KeyTo1LevelZenodoByAnchor keyTo1LevelZenodoByAnchor = new KeyTo1LevelZenodoByAnchor(new KeyToPath() {
            @Override
            public URI toPath(IRI key) {
                return URI.create("hash://md5/b871e22f0e8c576305f99cb5aff8cddd");
            }

            @Override
            public boolean supports(IRI key) {
                return true;
            }
        }, anchor);

        IRI actual = RefNodeFactory.toIRI(keyTo1LevelZenodoByAnchor.toPath(
                RefNodeFactory.toIRI("hash://sha256/5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03"))
        );

        IRI expected = RefNodeFactory.toIRI("zip:hash://md5/b871e22f0e8c576305f99cb5aff8cddd!/data/58/91/5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03");
        assertThat(
                actual.getIRIString(),
                Is.is(expected.getIRIString())
        );
    }

}