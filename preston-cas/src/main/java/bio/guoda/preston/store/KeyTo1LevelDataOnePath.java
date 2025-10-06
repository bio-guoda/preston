package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import bio.guoda.preston.RefNodeFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class KeyTo1LevelDataOnePath implements KeyToPath {

    public static final String DATAONE_URL_PREFIX = "https://cn.dataone.org/cn/v2/query/solr/?q=checksum:";
    public static final List<HashType> HASH_TYPES_SUPPORTED = Arrays.asList(HashType.md5, HashType.sha256, HashType.sha1);
    private final URI remote;
    private final Dereferencer<InputStream> deref;

    public KeyTo1LevelDataOnePath(URI remote, Dereferencer<InputStream> deref) {
        this.remote = detectPath(remote);
        this.deref = deref;
    }


    @Override
    public URI toPath(IRI key) {
        URI path = null;
        HashType hashType = HashKeyUtil.hashTypeFor(key);
        int offset = hashType.getPrefix().length();
        String contentId = StringUtils.substring(key.getIRIString(), offset);
        if (StringUtils.startsWith(remote.toString(), DATAONE_URL_PREFIX)) {
            IRI query = RefNodeFactory.toIRI(remote.toString() + contentId + "&fl=identifier,size,formatId,checksum,checksumAlgorithm,replicaMN,dataUrl&rows=10&wt=json");
            try (InputStream inputStream = deref.get(query)) {
                URI dataEntryURI = findFirstHit(contentId, inputStream);
                try (InputStream dataEntryStream = deref.get(RefNodeFactory.toIRI(dataEntryURI))) {
                    String entries = IOUtils.toString(dataEntryStream, StandardCharsets.UTF_8);
                    String[] split = StringUtils.splitByWholeSeparator(entries, "<url>");
                    if (split.length > 1) {
                        String pathString = StringUtils.trim(
                                StringUtils.splitByWholeSeparator(split[1], "</url>")[0]);
                        path = new URI(pathString);
                    }
                }
            } catch (IOException | URISyntaxException e) {
                // opportunistic
            }
        }
        if (path == null) {
            path = HashKeyUtil.insertSlashIfNeeded(remote, contentId);
        }
        return path;
    }

    private static URI findFirstHit(String suffix, InputStream inputStream) throws IOException, URISyntaxException {
        List<URI> iris = parseResponse(inputStream);
        if (iris.size() == 0) {
            throw new IOException("dataone does not have content [" + suffix + "]");
        }
        return iris.get(0);
    }

    @Override
    public boolean supports(IRI key) {
        return HASH_TYPES_SUPPORTED.contains(HashKeyUtil.hashTypeFor(key));
    }

    private static URI detectPath(URI baseURI) {
        return StringUtils.contains(baseURI.getHost(), "dataone.org")
                && (StringUtils.isBlank(baseURI.getPath()) || StringUtils.equals(baseURI.getPath(), "/"))
                ? URI.create(DATAONE_URL_PREFIX)
                : baseURI;
    }

    public static List<URI> parseResponse(InputStream is) throws IOException, URISyntaxException {
        JsonNode resp = new ObjectMapper().readTree(is);

        List<URI> candidates = new ArrayList<>();

        if (resp.has("response")) {
            JsonNode responseNode = resp.get("response");
            if (responseNode.has("docs")) {
                JsonNode docsNode = responseNode.get("docs");
                for (JsonNode doc : docsNode) {
                    if (doc.has("dataUrl")) {
                        candidates.add(new URI(doc.get("dataUrl").asText()));
                    }
                }
            }
        }
        return candidates;
    }


}
