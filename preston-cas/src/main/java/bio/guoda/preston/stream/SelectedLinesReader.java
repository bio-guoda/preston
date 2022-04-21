package bio.guoda.preston.stream;

import org.apache.commons.io.input.AbstractCharacterFilterReader;

import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

public class SelectedLinesReader extends AbstractCharacterFilterReader {
    private final Iterator<Long> lineNumberIterator;
    private long currentLine;
    private long seekLine;
    private boolean done;
    private long markedCurrentLine;
    private long markedSeekLine;
    private final Queue<Long> seekLinesSinceMark;

    public SelectedLinesReader(Iterator<Long> lineNumberIterator, Reader reader) {
        super(reader);
        this.lineNumberIterator = lineNumberIterator;
        this.currentLine = 1;
        this.seekLine = -1;
        seekLinesSinceMark = new LinkedList<>();
        updateSeekLine();
    }

    private void updateSeekLine() {
        if (lineNumberIterator.hasNext()) {
            seekLine = seekLinesSinceMark.isEmpty() ? lineNumberIterator.next() : seekLinesSinceMark.poll();
            if (markedCurrentLine > 0) {
                seekLinesSinceMark.add(seekLine);
            }
        } else {
            done = true;
        }
    }

    @Override
    protected boolean filter(int ch) {
        if (done) {
            return true;
        } else {
            boolean omit = currentLine < seekLine;
            if (ch == '\n') {
                ++currentLine;
                if (currentLine > seekLine) {
                    updateSeekLine();
                }
            }
            return omit || done;
        }
    }

    @Override
    public int read() throws IOException {
        return done ? -1 : super.read();
    }

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
        return done ? -1 : super.read(cbuf, off, len);
    }

    @Override
    public boolean ready() throws IOException {
        return super.ready() && !done;
    }

    @Override
    public long skip(long n) throws IOException {
        long i = 0;
        while (i < n) {
            int numCharsRead = read();
            if (numCharsRead > 0) {
                i += numCharsRead;
            } else {
                break;
            }
        }
        return i;
    }

    @Override
    public void mark(int readAheadLimit) throws IOException {
        super.mark(readAheadLimit);
        markedCurrentLine = currentLine;
        markedSeekLine = seekLine;
    }

    @Override
    public void reset() throws IOException {
        super.reset();
        if (markedCurrentLine > 0) {
            currentLine = markedCurrentLine;
            seekLine = markedSeekLine;
            markedCurrentLine = -1;
        }
    }
}
