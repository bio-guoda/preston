package bio.guoda.preston.util;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.process.StatementEmitter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;

import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;

/**
 * Used to page through search result pages using URL query params "limit" and "offset".
 *
 */

public class ResultPagerUtil {

    public static void emitPageRequests(IRI resultPage, Long recordsTotal, Long recordsFound, StatementEmitter emitter) {
        final Long limit = queryValueParamOrDefault(resultPage, "limit", recordsFound);

        if (moreRecordsAvailable(recordsTotal, recordsFound)
                && hasValidRequestPageLimit(limit)
                && noOffsetDefined(resultPage)) {
            Long recordLeft = recordsTotal - recordsFound;
            final long numberOfPages = recordLeft / limit;
            for (int page = 0; page <= numberOfPages; page++) {
                final IRI iri = queryForPage(resultPage, recordsFound + page * limit, limit);
                emitter.emit(RefNodeFactory.toStatement(iri, HAS_VERSION, RefNodeFactory.toBlank()));
            }
        }
    }

    private static boolean noOffsetDefined(IRI resultPage) {
        return !paramMatcher(resultPage, "offset").find();
    }

    private static boolean hasValidRequestPageLimit(Long limit) {
        return limit > 0;
    }

    private static boolean moreRecordsAvailable(Long recordsTotal, Long recordsFound) {
        return recordsFound < recordsTotal;
    }

    public static IRI queryForPage(IRI aPage, Long offset, Long limit) {
        final String pageIRIStripped = aPage.getIRIString()
                .trim()
                .replaceAll("limit=\\d+", "")
                .replaceAll("offset=\\d+", "")
                .replaceAll("&{2,}$", "")
                .replaceAll("\\?$", "");

        final String rawQuery = URI.create(pageIRIStripped).getRawQuery();
        String firstQueryParamSeparator = StringUtils.isBlank(rawQuery)
                ? "?"
                : (StringUtils.endsWith(rawQuery, "&") ? "" : "&");

        final URI nextPageRequest = URI.create(pageIRIStripped + firstQueryParamSeparator +
                "limit=" + limit +
                "&offset=" + offset);

        return RefNodeFactory.toIRI(nextPageRequest);
    }

    public static Long queryValueParamOrDefault(IRI request, String queryParamName, Long queryParamDefaultValue) {
        final Matcher matcher = paramMatcher(request, queryParamName);
        final boolean hasMatch = matcher.find();
        return hasMatch ? Long.parseLong(matcher.group(3)) : queryParamDefaultValue;
    }

    public static Matcher paramMatcher(IRI aPage, String queryParamName) {
        final String query = aPage.getIRIString();

        final Pattern OFFSET_PATTERN = Pattern.compile("(.*)(" + queryParamName + "=)(\\d+)(.*)");
        return OFFSET_PATTERN.matcher(query);
    }
}
