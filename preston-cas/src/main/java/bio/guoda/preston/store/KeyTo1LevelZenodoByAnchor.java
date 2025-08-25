package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import bio.guoda.preston.RefNodeFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;

import java.net.URI;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KeyTo1LevelZenodoByAnchor implements KeyToPath {

    private final KeyToPath anchoredDepositLookup;

    private final Map<IRI, URI> fileURIForAnchor = new TreeMap<>();
    private final IRI anchor;

    public KeyTo1LevelZenodoByAnchor(KeyToPath keyToPath, IRI anchor) {
        this.anchoredDepositLookup = keyToPath;
        this.anchor = anchor;
    }

    @Override
    public URI toPath(IRI key) {
        URI path = null;
        if (anchor != null) {
            HashType anchorType = HashKeyUtil.hashTypeFor(anchor);
            if (HashType.md5.equals(anchorType)) {
                URI fileURI = anchoredDepositLookup.toPath(anchor);
                if (fileURI != null) {
                    System.out.println("found archive hash candidate: [" + fileURI + "]");
                    IRI iri = RefNodeFactory.toIRI(fileURI);
                    if (HashKeyUtil.isValidHashKey(iri)) {
                        HashType keyType = HashKeyUtil.hashTypeFor(key);
                        Matcher matcher = keyType.getIRIPattern().matcher(key.getIRIString());
                        if (matcher.matches()) {
                            String hexString = StringUtils.substring(
                                    matcher.group("contentId"),
                                    keyType.getPrefix().length()
                            );
                            path = URI.create("zip:" + iri.getIRIString()+ "!/data"
                                            + "/" + StringUtils.substring(hexString, 0, 2)
                                            + "/" + StringUtils.substring(hexString, 2, 4)
                                            + "/" + hexString);
                            System.out.println("attempting: [" + path + "]");
                        }
                    }
                }
            }
        }
        return path;
    }

    public static String parseZenodoFilesEndpoint(URI fileURI) {
        String filesEndpoint = null;
        String uriString = fileURI.toString();
        Matcher matcher = Pattern.compile("https://zenodo.org.*(?<filesEndpoint>/records/[0-9]+/files/).*").matcher(uriString);
        if (matcher.matches()) {
            filesEndpoint = "https://zenodo.org" + matcher.group("filesEndpoint");
        }
        return filesEndpoint;
    }

    @Override
    public boolean supports(IRI key) {
        return HashType.md5.equals(HashKeyUtil.hashTypeFor(key));
    }


}
