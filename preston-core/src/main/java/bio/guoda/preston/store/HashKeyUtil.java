package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import bio.guoda.preston.RefNodeFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static bio.guoda.preston.RefNodeFactory.toIRI;

public class HashKeyUtil {

    public static final String PREFIX_SCHEMA = "([a-zA-Z0-9]{2,}:)";
    public static final String SUFFIX_SCHEMA = "(!/.*){0,1}";

    public static final Pattern URI_PATTERN_HASH_URI_COMPOSITE_SHA256 = Pattern
            .compile(PREFIX_SCHEMA + "*(" + HashType.sha256.getIRIPatternString() + "){1}" + SUFFIX_SCHEMA);

    public static final Pattern URI_PATTERN_HASH_URI_COMPOSITE_MD5 = Pattern
            .compile(PREFIX_SCHEMA + "*(" + HashType.md5.getIRIPatternString() + "){1}" + SUFFIX_SCHEMA);

    public static final Pattern URI_PATTERN_HASH_URI_COMPOSITE_SHA1 = Pattern
            .compile(PREFIX_SCHEMA + "*(" + HashType.sha1.getIRIPatternString() + "){1}" + SUFFIX_SCHEMA);

    public static final Pattern URI_PATTERN_URI_COMPOSITE = Pattern
            .compile(PREFIX_SCHEMA + "+([^!:]*)(!/.*){0,1}");
    public static final Pattern COMPOSITE_URI = Pattern
            .compile(PREFIX_SCHEMA + "+([^!:]*)(!/.*){0,1}");


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

    public static boolean isValidPlainHashKey(IRI hashKey) {
        boolean isPlainHashKey = false;
        String iriString = hashKey.getIRIString();

        HashType type = hashTypeFor(iriString);

        if (type != null) {
            Matcher matcher = type
                    .getIRIPattern()
                    .matcher(iriString);
            if (matcher.matches()) {
                isPlainHashKey =
                        StringUtils.isBlank(matcher.group("prefix"))
                        && StringUtils.isBlank(matcher.group("suffix"));
            }
        }

        return isPlainHashKey;
    }

    public static HashType hashTypeFor(IRI contentId) {
        return hashTypeFor(contentId.getIRIString());
    }


    public static HashType hashTypeFor(String iriString) {
        HashType type = null;

        try {
            if (HashType.sha256.getIRIPattern().matcher(iriString).matches()) {
                type = HashType.sha256;
            } else if (HashType.md5.getIRIPattern().matcher(iriString).matches()) {
                type = HashType.md5;
            } else if (HashType.sha1.getIRIPattern().matcher(iriString).matches()) {
                type = HashType.sha1;
            }
        } catch (IllegalArgumentException ex) {
            // ignore parsing error
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
                .matcher(comboHashURI.getIRIString()).matches()
                || URI_PATTERN_HASH_URI_COMPOSITE_SHA1
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

    public static IRI extractInnerURI(IRI uriString) {
        Matcher matcher = getMatcher(uriString.getIRIString());
        String innerURIString = null;
        if (matcher.matches()) {
            String innerScheme = matcher.group(1);
            String innerPath = matcher.group(2);
            if (StringUtils.isNotBlank(innerPath)) {
                innerURIString = innerScheme + innerPath;
            }
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
                    ? toIRI(contentHashMatcher.group("contentId"))
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

    public static HashType getHashTypeOrThrowIOException(IRI key) throws IOException {
        HashType hashTypeProvided = hashTypeFor(key);
        if (hashTypeProvided == null) {
            throw new IOException("found unsupported or invalid hash key [" + key + "]");
        }
        return hashTypeProvided;
    }

    public static String pathSuffixForKey(IRI key, HashType type1) {
        String keyStr = key.getIRIString();

        int offset = type1.getPrefix().length();
        String u0 = keyStr.substring(offset + 0, offset + 2);
        String u1 = keyStr.substring(offset + 2, offset + 4);

        String suffix = StringUtils.join(Arrays.asList(u0, u1, keyStr.substring(offset)), "/");
        return suffix;
    }
}
