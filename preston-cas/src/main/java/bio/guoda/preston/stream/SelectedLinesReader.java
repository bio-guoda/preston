package bio.guoda.preston.stream;

import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

public class SelectedLinesReader extends Reader {
    private final Iterator<Long> lineNumberIterator;
    private long currentLine;
    private long seekLine;
    private boolean done;
    private long markedCurrentLine;
    private long markedSeekLine;
    private final Queue<Long> seekLinesSinceMark;
    private final Reader in;

    public SelectedLinesReader(Iterator<Long> lineNumberIterator, Reader reader) {
        super(reader);
        this.lineNumberIterator = lineNumberIterator;
        in = reader;
        currentLine = 1;
        seekLine = -1;
        seekLinesSinceMark = new LinkedList<>();
        updateSeekLine();
    }

    private void updateSeekLine() {
        if (!getMarked() && !seekLinesSinceMark.isEmpty()) {
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
    public int read() throws IOException {
        while (!done) {
            int ch = this.in.read();
            if (ch == -1) {
                done = true;
            } else {
                boolean print = currentLine == seekLine;
                if (ch == '\n') {
                    ++currentLine;
                    if (currentLine > seekLine) {
                        updateSeekLine();
                    }
                }

                // To preserve existing behavior, do not print the final newline character
                if (print && !done) {
                    return ch;
                }
            }
        }

        return -1;
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
        return !done && super.ready();
    }

    @Override
    public long skip(long n) throws IOException {
        long i = 0;
        while (i < n && read() != -1) {
            ++i;
        }
        return i;
    }

    private boolean getMarked() {
        return markedCurrentLine > -1;
    }

    @Override
    public boolean markSupported() {
        return in.markSupported();
    }

    @Override
    public void mark(int readAheadLimit) throws IOException {
        in.mark(readAheadLimit);
        markedCurrentLine = currentLine;
        markedSeekLine = seekLine;
        seekLinesSinceMark.clear();
    }

    @Override
    public void reset() throws IOException {
        in.reset();
        if (markedCurrentLine > 0) {
            currentLine = markedCurrentLine;
            seekLine = markedSeekLine;
            markedCurrentLine = -1;
        } else {
            throw new IOException("can't reset reader without marking");
        }
    }

    @Override
    public void close() throws IOException {
        in.close();
    }
}