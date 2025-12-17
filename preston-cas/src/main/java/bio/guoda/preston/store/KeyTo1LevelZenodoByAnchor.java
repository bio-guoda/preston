package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import bio.guoda.preston.RefNodeFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;

public class KeyTo1LevelZenodoByAnchor implements KeyToPath {

    private final static Logger LOG = LoggerFactory.getLogger(KeyTo1LevelZenodoByAnchor.class);

    private final KeyToPath anchoredDepositLookup;

    private final IRI anchor;

    public KeyTo1LevelZenodoByAnchor(KeyToPath keyToPath, IRI anchor) {
        this.anchoredDepositLookup = keyToPath;
        this.anchor = anchor;
    }

    @Override
    public URI toPath(IRI key) {
        URI alias = null;
        if (anchor != null) {
            HashType anchorType = HashKeyUtil.hashTypeFor(anchor);
            if (HashType.md5.equals(anchorType)) {
                URI archiveURI = anchoredDepositLookup.toPath(anchor);
                if (archiveURI != null) {
                    IRI archiveIRI = RefNodeFactory.toIRI(archiveURI);
                    HashType keyType = HashKeyUtil.hashTypeFor(key);
                    Matcher matcher = keyType.getIRIPattern().matcher(key.getIRIString());
                    if (matcher.matches()) {
                        String hexString = StringUtils.substring(
                                matcher.group("contentId"),
                                keyType.getPrefix().length()
                        );
                        alias = URI.create("zip:" + archiveIRI.getIRIString() + "!/data"
                                + "/" + StringUtils.substring(hexString, 0, 2)
                                + "/" + StringUtils.substring(hexString, 2, 4)
                                + "/" + hexString);
                    }
                }
            }
        }
        return alias;
    }

    @Override
    public boolean supports(IRI key) {
        return anchor != null
                && HashType.md5.equals(HashKeyUtil.hashTypeFor(anchor))
                && HashKeyUtil.isValidHashKey(key);
    }


}
