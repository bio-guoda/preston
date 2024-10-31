package bio.guoda.preston.process;

import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.common.PDPageLabelRange;

public class SelectedPage {
    private final PDPage page;
    private final int index;
    private final PDPageLabelRange pageLabelRange;

    public SelectedPage(PDPage page, int index, PDPageLabelRange pageLabelRange) {
        this.page = page;
        this.index = index;
        this.pageLabelRange = pageLabelRange;
    }

    public PDPageLabelRange getPageLabelRange() {
        return pageLabelRange;
    }

    public int getIndex() {
        return index;
    }

    public PDPage getPage() {
        return page;
    }
}
