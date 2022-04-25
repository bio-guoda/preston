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
        if (!seekLinesSinceMark.isEmpty()) {
            seekLine = seekLinesSinceMark.poll();
        } else if (lineNumberIterator.hasNext()) {
            seekLine = lineNumberIterator.next();
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
        int i = off;
        int end = off + len;
        while (i < end) {
            int ch = read();
            if (ch != -1) {
                cbuf[i++] = (char) ch;
            } else {
                break;
            }
        }
        return i > off ? i - off : -1;
    }

    @Override
    public boolean ready() throws IOException {
        return super.ready() && !done;
    }

    @Override
    public long skip(long n) throws IOException {
        long i = 0;
        while (i < n && read() != -1) {
            ++i;
        }
        return i;
    }

    @Override
    public void mark(int readAheadLimit) throws IOException {
        super.mark(readAheadLimit);
        markedCurrentLine = currentLine;
        markedSeekLine = seekLine;
        seekLinesSinceMark.clear();
    }

    @Override
    public void reset() throws IOException {
        super.reset();
        if (markedCurrentLine > 0) {
            currentLine = markedCurrentLine;
            seekLine = markedSeekLine;
            markedCurrentLine = -1;
        } else {
            throw new IOException("can't reset reader without marking");
        }
    }
}
