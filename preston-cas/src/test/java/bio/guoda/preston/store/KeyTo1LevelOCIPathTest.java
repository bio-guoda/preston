package bio.guoda.preston.store;

import bio.guoda.preston.RefNodeFactory;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

public class KeyTo1LevelOCIPathTest {

    @Test
    public void pull() {
        //curl -fL --header "Authorization: Bearer QQ=="  "https://ghcr.io/v2/cboettig/content-store/blobs/sha256:9412325831dab22aeebdd674b6eb53ba6b7bdd04bb99a4dbb21ddff646287e37" -o test.txt

        KeyToPath key2path = new KeyTo1LevelOCIPath(URI.create("bla:foo"));

        URI uri = key2path.toPath(RefNodeFactory.toIRI("hash://sha256/b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"));
        assertThat(uri.toString(), Is.is("https://ghcr.io/v2/cboettig/content-store/blobs/sha256:b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"));
    }

    @Test
    public void ghcrRemotePattern() {

        Pattern compile = KeyTo1LevelOCIPath.GITHUB_CONTENT_REPOSITORY;

        String repositoryName = "https://ghcr.io/cboettig/content-store";
        Matcher matcher = compile.matcher(repositoryName);
        assertTrue(matcher.matches());
        assertEquals(matcher.group("org"), "cboettig");
        assertEquals(matcher.group("repo"), "content-store");
    }

    @Test
    public void ghcrRemotePattern2() {

        Pattern compile = KeyTo1LevelOCIPath.GITHUB_CONTENT_REPOSITORY;

        String repositoryName = "https://ghcr.io/v2/cboettig/content-store/blob";
        Matcher matcher = compile.matcher(repositoryName);
        assertTrue(matcher.matches());
        assertEquals(matcher.group("org"), "cboettig");
        assertEquals(matcher.group("repo"), "content-store");
    }

    @Test
    public void ghcrRemotePattern3() {
        Pattern compile = KeyTo1LevelOCIPath.GITHUB_CONTENT_REPOSITORY;
        String repositoryName = "ghcr.io/cboettig/content-store";
        Matcher matcher = compile.matcher(repositoryName);
        assertTrue(matcher.matches());
        assertEquals(matcher.group("org"), "cboettig");
        assertEquals(matcher.group("repo"), "content-store");
    }


}