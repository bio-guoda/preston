package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import bio.guoda.preston.RefNodeFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class KeyTo1LevelWikiMediaCommonsPath implements KeyToPath {

    public static final String WIKIMEDIA_COMMONS_ENDPOINT = "https://commons.wikimedia.org/w/api.php?action=query&list=allimages&format=json&aisha1=";
    private final URI remote;
    private static final Logger LOG = LoggerFactory.getLogger(KeyTo1LevelWikiMediaCommonsPath.class);
    private final Dereferencer<InputStream> deref;

    public KeyTo1LevelWikiMediaCommonsPath(URI remote, Dereferencer<InputStream> deref) {
        this.remote = detectPath(remote);
        this.deref = deref;
    }

    @Override
    public URI toPath(IRI key) {
        HashKeyUtil.validateHashKey(key);

        String keyStr = key.getIRIString();
        HashType hashType = HashKeyUtil.hashTypeFor(key);
        int offset = hashType.getPrefix().length();

        final String s = remote.toString();

        String suffix = keyStr.substring(offset);
        String path = StringUtils.join(Arrays.asList(s, suffix), "");


        URI query = StringUtils.equals(s, WIKIMEDIA_COMMONS_ENDPOINT)
                ? URI.create(path)
                : HashKeyUtil.insertSlashIfNeeded(remote, suffix);

        try {
            return resolveFirstCandidateURI(query);
        } catch (IOException | URISyntaxException e) {
            LOG.warn("failed to lookup [" + key.getIRIString() + "]", e);
            return null;
        }
    };

    private URI resolveFirstCandidateURI(URI query) throws IOException, URISyntaxException {
        try (InputStream inputStream = deref.get(RefNodeFactory.toIRI(query))) {
            List<URI> uris = parseResponse(inputStream);
            return (uris != null && uris.size() > 0) ? uris.get(0) : null;
        }
    }

    @Override
    public boolean supports(IRI key) {
        return HashType.sha1.equals(HashKeyUtil.hashTypeFor(key));
    }


    static List<URI> parseResponse(InputStream is) throws IOException, URISyntaxException {
        JsonNode resp = new ObjectMapper().readTree(is);

        List<URI> candidates = new ArrayList<>();

        if (resp.has("query")) {
            JsonNode responseNode = resp.get("query");
            if (responseNode.has("allimages")) {
                JsonNode docsNode = responseNode.get("allimages");
                for (JsonNode doc : docsNode) {
                    if (doc.has("url")) {
                        candidates.add(new URI(doc.get("url").asText()));
                    }
                }
            }
        }
        return candidates;
    }

    private static URI detectPath(URI baseURI) {
        return StringUtils.contains(baseURI.getHost(), "wikimedia.org")
                && (StringUtils.equals(baseURI.getPath(), "/") || StringUtils.isBlank(baseURI.getPath()))
                ? URI.create(WIKIMEDIA_COMMONS_ENDPOINT)
                : baseURI;
    }

}
