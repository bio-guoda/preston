package bio.guoda.preston.index;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;

import java.io.IOException;

public class TokenizerTLSH extends Tokenizer {
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

    private static final int NUM_BINS_PER_CHAR = 2;

    private byte binPair; // Two 2-bit bins are packed into each TLSH string character
    private int binNumber;
    private int offset;

    @Override
    public final boolean incrementToken() throws IOException {
        clearAttributes();
        String term;

        if (binNumber % NUM_BINS_PER_CHAR == 0) {
            final int readValue = input.read();
            if (readValue == -1) {
                return false;
            }
            offset += 1;

            binPair = (byte)Character.digit((char)readValue, 16);
            term = GetTermForBin(GetFirstBinValue(binPair), binNumber);
        } else {
            term = GetTermForBin(GetSecondBinValue(binPair), binNumber);
        }

        char[] termBuffer = term.toCharArray();
        termAtt.copyBuffer(termBuffer, 0, termBuffer.length);

        offsetAtt.setOffset(correctOffset(offset - 1), correctOffset(offset));

        binNumber += 1;

        return true;
    }

    private static int GetFirstBinValue(int pair) {
        return (pair & 0b1100) >> 2;
    }

    private static int GetSecondBinValue(int pair) {
        return pair & 0b0011;
    }

    private static String GetTermForBin(int binValue, int binNumber) {
        int termValue = (binNumber << 2) | binValue;
        return Integer.toString(termValue);
    }

    public void reset() throws IOException {
        super.reset();
        binNumber = 0;
        offset = 0;
    }

    public final void end() throws IOException {
        super.end();
        int finalOffset = correctOffset(offset);
        this.offsetAtt.setOffset(finalOffset, finalOffset);
    }
}
