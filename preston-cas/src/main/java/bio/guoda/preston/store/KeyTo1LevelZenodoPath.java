package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import bio.guoda.preston.RefNodeFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public class KeyTo1LevelZenodoPath implements KeyToPath {

    public static final String ZENODO_API_PREFIX = "https://zenodo.org/api/records/?q=_files.checksum:";
    private final URI baseURI;
    private final Dereferencer<InputStream> deref;

    public KeyTo1LevelZenodoPath(URI baseURI, Dereferencer<InputStream> deref) {
        this.baseURI = detectPath(baseURI);
        this.deref = deref;
    }

    @Override
    public URI toPath(IRI key) {
        URI path = null;
        HashType hashType = HashKeyUtil.hashTypeFor(key);
        int offset = hashType.getPrefix().length();
        String md5HexHash = StringUtils.substring(key.getIRIString(), offset);
        if (StringUtils.startsWith(baseURI.toString(), ZENODO_API_PREFIX)) {
            IRI zenodoQuery = RefNodeFactory.toIRI(baseURI.toString() + "%22md5:" + md5HexHash + "%22&all_versions=true");
            try (InputStream inputStream = deref.get(zenodoQuery)) {
                path = findFirstHit(md5HexHash, inputStream);
            } catch (IOException e) {
                // opportunistic
            }
        }
        if (path == null) {
            path = HashKeyUtil.insertSlashIfNeeded(baseURI, md5HexHash);
        }
        return path;
    }

    static URI findFirstHit(String suffix, InputStream inputStream) throws IOException {
        if (inputStream == null) {
            throw new IOException("no input found");
        }
        JsonNode jsonNode = new ObjectMapper().readTree(inputStream);

        if (jsonNode != null && jsonNode.has("hits")) {
            JsonNode hits = jsonNode.get("hits");
            if (hits.has("hits")) {
                JsonNode moreHits = hits.get("hits");
                for (JsonNode hit : moreHits) {
                    if (hit.has("files")) {
                        JsonNode files = hit.get("files");
                        for (JsonNode file : files) {
                            if (file.has("checksum")) {
                                String checksum = file.get("checksum").asText();
                                if (StringUtils.equals("md5:" + suffix, checksum)) {
                                    if (file.has("links")) {
                                        JsonNode links = file.get("links");
                                        if (links.has("self")) {
                                            return URI.create(links.get("self").asText());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return null;
    }

    @Override
    public boolean supports(IRI key) {
        return HashType.md5.equals(HashKeyUtil.hashTypeFor(key));
    }

    private static URI detectPath(URI baseURI) {
        return StringUtils.contains(baseURI.getHost(), "zenodo.org")
                && (StringUtils.isBlank(baseURI.getPath()) || StringUtils.equals(baseURI.getPath(), "/"))
                ? URI.create(ZENODO_API_PREFIX)
                : baseURI;
    }

}
