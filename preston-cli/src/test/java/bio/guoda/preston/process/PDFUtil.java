package bio.guoda.preston.process;

import bio.guoda.preston.stream.ContentStreamException;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDDocumentCatalog;
import org.apache.pdfbox.pdmodel.common.PDPageLabelRange;
import org.apache.pdfbox.pdmodel.common.PDPageLabels;

import java.io.IOException;

public class PDFUtil {
    public static Pair<Integer, PDPageLabelRange> getPageIndexAndPageLabelRange(String requestedPageLabel, IRI contentId, PDDocument pdDocument) throws IOException, ContentStreamException {
        PDDocumentCatalog documentCatalog = pdDocument.getDocumentCatalog();

        PDPageLabels pageLabels = documentCatalog.getPageLabels();
        int pageIndex;
        PDPageLabelRange pageLabelRange;
        if (useIndexAsLabel(requestedPageLabel, pageLabels)) {
            int requestedPageNumber = Integer.parseInt(requestedPageLabel);
            pageIndex = requestedPageNumber - 1;
            pageLabelRange = new PDPageLabelRange();
            pageLabelRange.setPrefix("");
            pageLabelRange.setStart(requestedPageNumber);
            pageLabelRange.setStyle(PDPageLabelRange.STYLE_DECIMAL);
        } else if (useLabel(requestedPageLabel, pageLabels)) {
            pageIndex = pageLabels.getPageIndicesByLabels().get(requestedPageLabel);
            pageLabelRange = pageLabels.getPageLabelRange(pageIndex);
        } else {
            throw new ContentStreamException("cannot find page [" + requestedPageLabel + "] in " + contentId.toString());
        }
        return Pair.of(pageIndex, pageLabelRange);
    }

    private static boolean useLabel(String pageLabel, PDPageLabels pageLabels) {
        return pageLabels != null && pageLabels.getPageIndicesByLabels().containsKey(pageLabel);
    }

    private static boolean useIndexAsLabel(String pageLabel, PDPageLabels pageLabels) {
        return pageLabels == null && NumberUtils.isDigits(pageLabel);
    }
}
