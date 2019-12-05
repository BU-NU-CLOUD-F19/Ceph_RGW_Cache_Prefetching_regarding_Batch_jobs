---
layout: global
title: TRUNCATE TABLE
displayTitle: TRUNCATE TABLE
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
 
     http://www.apache.org/licenses/LICENSE-2.0
 
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

### Description
The `TRUNCATE TABLE` statement removes all the rows from a table or partition(s). The table must not be a view 
or an external/temporary table. In order to truncate multiple partitions at once, the user can specify the partitions 
in `partition_spec`. If no `partition_spec` is specified it will remove all partitions in the table.

### Syntax
{% highlight sql %}
TRUNCATE TABLE table_name [PARTITION partition_spec];
{% endhighlight %}

### Parameters
<dl>
  <dt><code><em>table_name</em></code></dt>
  <dd>The name of an existing table.</dd>
</dl>

<dl>
  <dt><code><em>PARTITION ( partition_spec :[ partition_column = partition_col_value, partition_column = partition_col_value, ...] )</em></code></dt>
  <dd>Specifies one or more partition column and value pairs. The partition value is optional.</dd>
</dl>


### Examples
{% highlight sql %}

--Create table Student with partition
CREATE TABLE Student ( name String, rollno INT) PARTITIONED BY (age int);

SELECT * from Student;
+-------+---------+------+--+
| name  | rollno  | age  |
+-------+---------+------+--+
| ABC   | 1       | 10   |
| DEF   | 2       | 10   |
| XYZ   | 3       | 12   |
+-------+---------+------+--+

-- Removes all rows from the table in the partion specified
TRUNCATE TABLE Student partition(age=10);

--After truncate execution, records belonging to partition age=10 are removed
SELECT * from Student;
+-------+---------+------+--+
| name  | rollno  | age  |
+-------+---------+------+--+
| XYZ   | 3       | 12   |
+-------+---------+------+--+

-- Removes all rows from the table from all partitions
TRUNCATE TABLE Student;

SELECT * from Student;
+-------+---------+------+--+
| name  | rollno  | age  |
+-------+---------+------+--+
+-------+---------+------+--+
No rows selected 

{% endhighlight %}


### Related Statements
- [DROP TABLE](sql-ref-syntax-ddl-drop-table.html)
- [ALTER TABLE](sql-ref-syntax-ddl-alter-tabley.html)

