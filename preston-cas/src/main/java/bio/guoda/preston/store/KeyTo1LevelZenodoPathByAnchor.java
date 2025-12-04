package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import bio.guoda.preston.RefNodeFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;

import java.net.URI;
import java.util.regex.Matcher;

public class KeyTo1LevelZenodoPathByAnchor implements KeyToPath {

    private final KeyToPath anchoredDepositLookup;

    private final IRI anchor;

    public KeyTo1LevelZenodoPathByAnchor(KeyToPath keyToPath, IRI anchor) {
        this.anchoredDepositLookup = keyToPath;
        this.anchor = anchor;
    }

    @Override
    public URI toPath(IRI key) {
        URI path = null;
        if (anchor != null) {
            HashType anchorType = HashKeyUtil.hashTypeFor(anchor);
            if (HashType.md5.equals(anchorType)) {
                URI anchoredArchiveContentId = anchoredDepositLookup.toPath(anchor);
                if (anchoredArchiveContentId != null) {
                    IRI archiveId = RefNodeFactory.toIRI(anchoredArchiveContentId);
                    if (HashKeyUtil.isValidHashKey(archiveId)) {
                        HashType keyType = HashKeyUtil.hashTypeFor(key);
                        Matcher matcher = keyType.getIRIPattern().matcher(key.getIRIString());
                        if (matcher.matches()) {
                            String hexString = StringUtils.substring(
                                    matcher.group("contentId"),
                                    keyType.getPrefix().length()
                            );
                            path = URI.create("zip:" + archiveId.getIRIString() + "!/data"
                                    + "/" + StringUtils.substring(hexString, 0, 2)
                                    + "/" + StringUtils.substring(hexString, 2, 4)
                                    + "/" + hexString);
                            // interrupt the content dereferencing chain using a runtime exception
                            // first pass at implementing https://github.com/bio-guoda/preston/issues/356
                            throw new ContentAlternateException(path);
                        }
                    }
                }
            }
        }
        return path;
    }

    @Override
    public boolean supports(IRI key) {
        return anchor != null
                && HashType.md5.equals(HashKeyUtil.hashTypeFor(anchor))
                && HashKeyUtil.isValidHashKey(key);
    }


}
