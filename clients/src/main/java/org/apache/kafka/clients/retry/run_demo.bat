@echo off
REM Simple script to compile and run the retry demo
REM This avoids the gradlew issues by using javac/java directly

echo Compiling retry classes...
javac -cp "..\..\..\..\..\build\libs\*" -d . *.java

if %errorlevel% neq 0 (
    echo Compilation failed. Make sure Kafka is built first.
    pause
    exit /b 1
)

echo.
echo Running demo...
java -cp ".;..\..\..\..\..\build\libs\*" org.apache.kafka.clients.retry.RetryDemo

echo.
echo Demo completed!
pause