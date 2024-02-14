package bio.guoda.preston.cmd;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

public final class JavaScriptAndPythonFriendlyURLEncodingUtil {
    public static String urlEncode(String str) {
        try {
            ScriptEngineManager manager = new ScriptEngineManager();
            ScriptEngine engine = manager.getEngineByName("JavaScript");
            return  (String) engine.eval("encodeURIComponent('" + str + "')");
        } catch (ScriptException e) {
            return str;
        }

    }
}
