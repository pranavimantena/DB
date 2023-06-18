# DB
DBMS implementation in c++

#db.h  -> this file has all headers in it
#db.cpp -> code
This DBMS will allow the user to type in simple DDL statements and build a system catalog (set of packed descriptors).  Once the table definition is in place, the user can insert, update, delete, and select from a table.  All the commands are passed into the CLP. 

1)  CREATE TABLE table_name (  { column_name <data_type> [NOT NULL] }  )
     <data_type> : INT, CHAR(n)


2)  DROP TABLE table_name


3)  LIST TABLE


4)  LIST SCHEMA FOR table_name [TO report_filename]


5)  INSERT INTO table_name VALUES (  { data_value }  )
     

6)  DELETE FROM table_name [ WHERE column_name <relational_operator> data_value ]


7)  UPDATE table_name SET column = data_value [ WHERE column_name <relational_operator> data_value ]


8)  SELECT { column_name } FROM table_name
	 [ WHERE column_name <condition> [(AND | OR) column_name <condition>] ]
       [ ORDER BY column_name [DESC] ]
	| 
	SELECT <aggregate>(column_name) FROM table_name
	 [ WHERE column_name <condition> [(AND | OR) column_name <condition>] ]
       [ ORDER BY column_name [DESC] ]
9) 
  
1) GROUP BY
	SELECT [column_name,] <aggregate>(column_name) FROM table_name
	 [ WHERE column_name <condition> [(AND | OR) column_name <condition>] ]
       [ GROUP BY column_name ]
	 [ ORDER BY column_name [DESC] ]

2)	NATURAL JOIN
	SELECT { [table_name.]column_name } FROM table_name [NATURAL JOIN table_name]
	 [ WHERE [table_name.]column_name <condition> [(AND | OR) [table_name.]column_name <condition>] ]
       [ ORDER BY column_name [DESC] ]
