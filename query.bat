@echo off
cd target
java -jar tdclientcommand-1.0-SNAPSHOT-shade.jar %*
pause
cd ..