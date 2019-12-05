---
layout: global
title: LIST JAR
displayTitle: LIST JAR
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
`LIST JAR` lists the JARs added by [ADD JAR](sql-ref-syntax-aux-resource-mgmt-add-jar.html).

### Syntax
{% highlight sql %}
LIST JAR
{% endhighlight %}

### Examples
{% highlight sql %}
ADD JAR /tmp/test.jar;
LIST JAR;
-- output for LIST JAR
spark://192.168.1.112:62859/jars/test.jar
{% endhighlight %}

### Related Statements
 * [ADD JAR](sql-ref-syntax-aux-resource-mgmt-add-jar.html)
 * [ADD FILE](sql-ref-syntax-aux-resource-mgmt-add-file.html)
 * [LIST FILE](sql-ref-syntax-aux-resource-mgmt-list-file.html)

