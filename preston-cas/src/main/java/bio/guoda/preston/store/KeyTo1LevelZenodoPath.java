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
    public static final String ZENODO_API_SUFFIX = "%22&all_versions=true";

    public static final String ZENODO_API_PREFIX_2023_10_13 = "https://zenodo.org/api/records?q=files.entries.checksum:";
    public static final String ZENODO_API_SUFFIX_2023_10_13 = "%22&allversions=1";
    private final URI baseURI;
    private final Dereferencer<InputStream> deref;
    private final String prefix;
    private final String suffix;

    public KeyTo1LevelZenodoPath(URI baseURI, Dereferencer<InputStream> deref) {
        this(baseURI, deref, ZENODO_API_PREFIX, ZENODO_API_SUFFIX);
    }

    public KeyTo1LevelZenodoPath(URI baseURI,
                                 Dereferencer<InputStream> deref,
                                 String prefix,
                                 String suffix) {
        this.deref = deref;
        this.prefix = prefix;
        this.suffix = suffix;
        this.baseURI = detectPath(baseURI);
    }

    @Override
    public URI toPath(IRI key) {
        URI path = null;
        HashType hashType = HashKeyUtil.hashTypeFor(key);
        int offset = hashType.getPrefix().length();
        String md5HexHash = StringUtils.substring(key.getIRIString(), offset);
        if (StringUtils.startsWith(baseURI.toString(), getPrefix())) {
            IRI zenodoQuery = RefNodeFactory.toIRI(baseURI.toString() + "%22md5:" + md5HexHash + getSuffix());
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

    private String getSuffix() {
        return suffix;
    }

    private String getPrefix() {
        return prefix;
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
                                if (StringUtils.equals(suffix, checksum)
                                        || StringUtils.equals("md5:" + suffix, checksum)) {
                                    if (file.has("links")) {
                                        JsonNode links = file.get("links");
                                        if (links.has("download")) {
                                            return URI.create(links.get("download").asText());
                                        } else if (links.has("self")) {
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

    private URI detectPath(URI baseURI) {
        return StringUtils.contains(baseURI.getHost(), "zenodo.org")
                && (StringUtils.isBlank(baseURI.getPath()) || StringUtils.equals(baseURI.getPath(), "/"))
                ? URI.create(getPrefix())
                : baseURI;
    }

}
