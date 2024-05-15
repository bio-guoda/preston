package bio.guoda.preston.cmd;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class JsonObjectDelineator {

    public static void locateTopLevelObjects(InputStream is, Consumer<Pair<Long, Long>> listener) throws IOException {

        JsonParser parser = new JsonFactory().createParser(is);

        JsonLocation objStartLocation = parser.getCurrentLocation();

        while (parser.nextToken() != null) {
            if (JsonToken.START_OBJECT.equals(parser.currentToken())) {
                objStartLocation = parser.getCurrentLocation();
                break;
            }
        }

        AtomicInteger depth = new AtomicInteger(1);
        while (parser.nextToken() != null) {
            if (JsonToken.START_OBJECT.equals(parser.currentToken())) {
                depth.incrementAndGet();
                if (objStartLocation == null) {
                    objStartLocation = parser.getCurrentLocation();
                }
            } else if (JsonToken.END_OBJECT.equals(parser.getCurrentToken())) {
                depth.decrementAndGet();
                if (depth.get() == 0 && objStartLocation != null) {
                    listener.accept(Pair.of(objStartLocation.getByteOffset(), parser.getCurrentLocation().getByteOffset()));
                    objStartLocation = null;
                }
            }
        }
    }
}
