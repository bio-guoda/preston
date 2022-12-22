package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import bio.guoda.preston.RefNodeFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;

import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static bio.guoda.preston.RefNodeFactory.toIRI;

public class HashKeyUtil {

    public static final Pattern URI_PATTERN_HASH_URI_COMPOSITE_SHA256 = Pattern
            .compile("([a-zA-Z0-9]+:)*(" + HashType.sha256.getIRIPatternString() + "){1}(!/.*){0,1}");

    public static final Pattern URI_PATTERN_HASH_URI_COMPOSITE_MD5 = Pattern
            .compile("([a-zA-Z0-9]+:)*(" + HashType.md5.getIRIPatternString() + "){1}(!/.*){0,1}");

    public static final Pattern URI_PATTERN_URI_COMPOSITE = Pattern
            .compile("([a-zA-Z0-9]+[:]{1})+([^!:]*)(!/.*){0,1}");
    public static final Pattern COMPOSITE_URI = Pattern
            .compile("([a-zA-Z0-9]+[:]{1})+([^!:]*)(!/.*){0,1}");


    public static void validateHashKey(IRI hashKey) {
        if (!isValidHashKey(hashKey)) {
            throw new IllegalArgumentException("found unsupported or invalid hash key [" + hashKey + "]");
        }
    }

    public static boolean isValidHashKey(IRI hashKey) {
        String iriString = hashKey.getIRIString();

        HashType type = hashTypeFor(iriString);

        return type != null
                && type
                .getIRIPattern()
                .matcher(iriString)
                .matches();
    }

    public static HashType hashTypeFor(IRI contentId) {
        return hashTypeFor(contentId.getIRIString());
    }


    public static HashType hashTypeFor(String iriString) {
        HashType type = null;

        if (StringUtils.startsWith(iriString, HashType.sha256.getPrefix())) {
            type = HashType.sha256;
        } else if (StringUtils.startsWith(iriString, HashType.md5.getPrefix())) {
            type = HashType.md5;
        }
        return type;
    }


    public static URI insertSlashIfNeeded(URI uri, String suffix) {
        String baseURI = uri.toString();
        return StringUtils.endsWith(baseURI, "/")
                ? URI.create(baseURI + suffix)
                : URI.create(baseURI + "/" + suffix);
    }

    public static boolean isLikelyCompositeHashURI(IRI comboHashURI) {
        return comboHashURI != null
                && (URI_PATTERN_HASH_URI_COMPOSITE_SHA256
                .matcher(comboHashURI.getIRIString()).matches()
                || URI_PATTERN_HASH_URI_COMPOSITE_MD5
                .matcher(comboHashURI.getIRIString()).matches());
    }

    public static boolean isLikelyCompositeURI(IRI comboHashURI) {
        return comboHashURI != null
                && URI_PATTERN_URI_COMPOSITE
                .matcher(comboHashURI.getIRIString()).matches();
    }

    public static Matcher getMatcher(String uriString) {
        return COMPOSITE_URI.matcher(uriString);
    }

    public static String extractInnerURI(String iriString) {
        return extractInnerURI(RefNodeFactory.toIRI(iriString)).getIRIString();
    }

    public static IRI extractInnerURI(IRI uriString) {
        Matcher matcher = getMatcher(uriString.getIRIString());
        String innerURIString = null;
        if (matcher.matches()) {
            String innerScheme = matcher.group(1);
            String innerPath = matcher.group(2);
            innerURIString = innerScheme + innerPath;
        }
        return StringUtils.isBlank(innerURIString)
                ? uriString
                : RefNodeFactory.toIRI(innerURIString);
    }

    public static IRI extractContentHash(IRI iri) throws IllegalArgumentException {
        IRI contentHash = null;
        for (HashType hashType : HashType.values()) {
            final Pattern contentHashPattern = hashType.getIRIPattern();
            Matcher contentHashMatcher = contentHashPattern.matcher(iri.getIRIString());

            contentHash = (contentHashMatcher.find())
                    ? toIRI(contentHashMatcher.group())
                    : null;

            if (contentHash != null) {
                break;
            }
        }

        if (contentHash == null) {
            throw new IllegalArgumentException("[" + iri.getIRIString() + "] is not a content-based URI (e.g. \"...hash://sha256/abc123...\"");
        } else {
            return contentHash;
        }
    }

    public static HashType getHashTypeOrThrow(IRI key) {
        HashType hashTypeProvided = hashTypeFor(key);
        if (hashTypeProvided == null) {
            throw new IllegalArgumentException("found unsupported or invalid hash key [" + key + "]");
        }
        return hashTypeProvided;
    }
}
