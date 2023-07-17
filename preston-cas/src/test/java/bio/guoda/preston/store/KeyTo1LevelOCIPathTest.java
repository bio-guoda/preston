package bio.guoda.preston.store;

import org.junit.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class KeyTo1LevelOCIPathTest {

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