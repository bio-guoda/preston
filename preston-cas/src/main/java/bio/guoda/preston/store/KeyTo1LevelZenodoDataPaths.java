package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.cmd.JavaScriptAndPythonFriendlyURLEncodingUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KeyTo1LevelZenodoDataPaths implements KeyToPath {

    public static final String ZENODO_API_BASE_PREFIX = "https://zenodo.org/api/records/?q=";
    public static final String ZENODO_API_PREFIX = ZENODO_API_BASE_PREFIX + "_files.checksum:";
    public static final String ZENODO_API_SUFFIX = "%22&all_versions=false&size=1";

    public static final String ZENODO_API_PREFIX_2023_10_13 = "https://zenodo.org/api/records?q=files.entries.checksum:";
    public static final String ZENODO_API_SUFFIX_2023_10_13 = "%22&allversions=0&size=1";
    private final URI remote;
    private final Dereferencer<InputStream> deref;
    private final String prefix;
    private final String suffix;

    public KeyTo1LevelZenodoDataPaths(URI remote, Dereferencer<InputStream> deref) {
        this(remote, deref, ZENODO_API_PREFIX, ZENODO_API_SUFFIX);
    }

    public KeyTo1LevelZenodoDataPaths(URI remote,
                                      Dereferencer<InputStream> deref,
                                      String prefix,
                                      String suffix) {
        this.deref = deref;
        this.prefix = prefix;
        this.suffix = suffix;
        this.remote = detectPath(remote);
    }

    @Override
    public URI toPath(IRI key) {
        URI path = null;
        HashType hashType = HashKeyUtil.hashTypeFor(key);
        int offset = hashType.getPrefix().length();
        String md5HexHash = StringUtils.substring(key.getIRIString(), offset);
        if (StringUtils.startsWith(remote.toString(), getPrefix())) {
            IRI zenodoQuery = RefNodeFactory.toIRI(remote.toString() + "%22md5:" + md5HexHash + getSuffix());
            try (InputStream inputStream = deref.get(zenodoQuery)) {
                path = findFirstHit(md5HexHash, inputStream);
                if (null == path) {
                    path = queryByPlainHashQuery(key, path, md5HexHash);
                }
            } catch (IOException e) {
                // opportunistic
            }
        }
        return path;
    }

    private URI queryByPlainHashQuery(IRI key, URI path, String md5HexHash) {
        IRI zenodoQueryPlainHash = RefNodeFactory.toIRI(ZENODO_API_BASE_PREFIX + "%22" + key.getIRIString() + getSuffix());
        try (InputStream inputStream2 = deref.get(zenodoQueryPlainHash)) {
            URI filesEndpoint = findFirstHit(md5HexHash, inputStream2);
            if (filesEndpoint != null) {
                path = pathToFirstMatchingEntry(path, md5HexHash, filesEndpoint);
            }
        } catch (IOException e) {
            // opportunistic
        }
        return path;
    }

    private URI pathToFirstMatchingEntry(URI path, String md5HexHash, URI filesEndpoint) {
        try (InputStream inputStream3 = deref.get(RefNodeFactory.toIRI(filesEndpoint))) {
            if (inputStream3 == null) {
                throw new IOException("no input found");
            }
            JsonNode jsonNode = new ObjectMapper().readTree(inputStream3);

            if (jsonNode != null && !jsonNode.at("/entries").isMissingNode()) {
                for (JsonNode entry : jsonNode.at("/entries")) {
                    JsonNode checksum = entry.at("/checksum");
                    if (!checksum.isMissingNode() && ("md5:" + md5HexHash).equals(checksum.asText())) {
                        JsonNode selfLink = jsonNode.at("/links/self");
                        if (!selfLink.isMissingNode()) {
                            path = contentLinkForFileEntry(entry, selfLink.asText());
                        }
                    }
                }
            }


        } catch (IOException | URISyntaxException e) {
            // opportunistic
        }
        return path;
    }

    static URI contentLinkForFileEntry(JsonNode entry, String prefix) throws URISyntaxException {
        URI path = null;
        JsonNode contentLink = entry.at("/key");
        if (!contentLink.isMissingNode()) {
            String linkText = contentLink.asText();
            path = URI.create(prefix
                    + "/"
                    + JavaScriptAndPythonFriendlyURLEncodingUtil.urlEncode(linkText)
                    + "/content"
            );
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
        return parseResult(suffix, jsonNode);
    }

    static URI parseResult(String suffix, JsonNode jsonNode) {
        URI uri = null;
        if (jsonNode != null) {
            JsonNode hits = jsonNode.at("/hits/hits");
                for (JsonNode hit : hits) {
                    JsonNode files = hit.at("/files");
                    for (JsonNode file : files) {
                        String filename = file.at("/key").asText();
                        if (StringUtils.equals(filename, "data.zip") && file.has("checksum")) {
                            JsonNode downloadLink = file.at("/links/self");
                            if (downloadLink.isTextual()) {
                                uri = URI.create(downloadLink.asText());
                            }
                        }
                    }
                }
        }
        return uri;
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
