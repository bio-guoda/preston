package bio.guoda.preston.stream;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;

public class CharBufferByteReader {
    private ByteBuffer byteBuffer;
    private CharBuffer charBuffer;
    private Charset charset;

    private int charPosition = 0;

    public CharBufferByteReader(ByteBuffer scanningByteBuffer, CharBuffer charBuffer, Charset charset) {
        this.byteBuffer = scanningByteBuffer;
        this.charBuffer = charBuffer;
        this.charset = charset;
    }

    public int advance(int newCharPosition) {
        ByteBuffer filteredByteBuffer = charset.encode(charBuffer.subSequence(this.charPosition, newCharPosition));

        int i = getBufferPosition(byteBuffer);
        for (int j = 0; i < byteBuffer.limit() && j < filteredByteBuffer.limit(); ++i) {
            if (byteBuffer.get(i) == filteredByteBuffer.get(j)) {
                ++j;
            }
        }

        setBufferPosition(byteBuffer, i);
        this.charPosition = newCharPosition;

        return i;
    }

    public static int getBufferPosition(Buffer buffer) {
        return buffer.position();
    }

    public static void setBufferPosition(Buffer buffer, int newPosition) {
        buffer.position(newPosition);
    }

}
