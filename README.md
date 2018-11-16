# TDClientCommand
TDClientQuery contains functions to query a certain database with parameters

Required parameter:
  -db <db_name>
  -tb <table_name>

Optional parameters:
	-l: limit is optional and specifies the limit of records returned. Read all records if not specified
	-m: -min is optional and specifies the minimum timestamp: NULL by default
	-M: -MAX is optional and specifies the maximum timestamp: NULL by default
	-e: engine is optional and specifies the query engine: ‘presto’ by default
	-c: -column is optional and specifies the comma separated list of columns to restrict the result to. Read all columns if not specified.
	-f: -format is optional and specifies the output format: tabular by default – has not been supported yet 

Build the source code:
  - Open windows command prompt
  - change directory to TDClientCommand folder
  - Run command "mvn clean install". Notes running that command also run unittest

Put td.conf under <your_folder>\.td\td.conf. Example: C:\Users\ttruong\.td\td.conf

Execute query: there are 3 ways
  - There are 2 batch files. You can run query_example.bat in windows command prompt as an example without entering any option or value
  - You run query.bat <your options and values> in indows command prompt
  - In windows command prompt, change directory to target then run the query with syntax: java -jar tdclientcommand-1.0-SNAPSHOT-shade.jar <your options and values>
  EX: java -jar tdclientcommand-1.0-SNAPSHOT-shade.jar -c items,passed,failed -l 10 -e presto -db automation_data -tb automation_result -m 1510480920 -M 154193046
  
