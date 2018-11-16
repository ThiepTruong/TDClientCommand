@echo off
cd target
java -jar tdclientcommand-1.0-SNAPSHOT-shade.jar -c items,passed,failed -l 10 -e presto -db automation_data -tb automation_result -m 1510480920 -M 1541930460
pause
cd ..