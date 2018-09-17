#!/usr/bin/env sh
@ 2>/dev/null # 2>nul & echo off & goto BOF
:
exec java -Xmx500m -XX:+UseG1GC $JAVA_OPTS -cp "$0" bio.guoda.preston.Preston "$@"
exit

:BOF
@echo off
java -Xmx500m -XX:+UseG1GC %JAVA_OPTS% -cp "%~dpnx0" bio.guoda.preston.Preston %*
exit /B %errorlevel%
