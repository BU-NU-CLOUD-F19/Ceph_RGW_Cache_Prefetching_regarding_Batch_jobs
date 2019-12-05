---
layout: global
title: LIST FILE
displayTitle: LIST FILE
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
`LIST FILE` lists the files added by [ADD FILE](sql-ref-syntax-aux-resource-mgmt-add-file.html).

### Syntax
{% highlight sql %}
LIST FILE
{% endhighlight %}

### Examples
{% highlight sql %}
ADD FILE /tmp/test;
LIST FILE;
-- output for LIST FILE
file:/private/tmp/test
{% endhighlight %}

### Related Statements
 * [ADD FILE](sql-ref-syntax-aux-resource-mgmt-add-file.html)
 * [ADD JAR](sql-ref-syntax-aux-resource-mgmt-add-jar.html)
 * [LIST JAR](sql-ref-syntax-aux-resource-mgmt-list-jar.html)

