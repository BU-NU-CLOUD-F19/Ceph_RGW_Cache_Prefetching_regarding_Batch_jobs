/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.expressions;

import com.google.common.annotations.VisibleForTesting;

/**
 * Expression information, will be used to describe a expression.
 */
public class ExpressionInfo {
    private String className;
    private String usage;
    private String name;
    private String extended;
    private String db;
    private String arguments;
    private String examples;
    private String note;
    private String since;
    private String deprecated;

    public String getClassName() {
        return className;
    }

    public String getUsage() {
        return replaceFunctionName(usage);
    }

    public String getName() {
        return name;
    }

    public String getExtended() {
        return replaceFunctionName(extended);
    }

    public String getSince() {
        return since;
    }

    public String getArguments() {
        return arguments;
    }

    @VisibleForTesting
    public String getOriginalExamples() {
        return examples;
    }

    public String getExamples() {
        return replaceFunctionName(examples);
    }

    public String getNote() {
        return note;
    }

    public String getDeprecated() {
        return deprecated;
    }

    public String getDb() {
        return db;
    }

    public ExpressionInfo(
            String className,
            String db,
            String name,
            String usage,
            String arguments,
            String examples,
            String note,
            String since,
            String deprecated) {
        assert name != null;
        assert arguments != null;
        assert examples != null;
        assert examples.isEmpty() || examples.contains("    Examples:");
        assert note != null;
        assert since != null;
        assert deprecated != null;

        this.className = className;
        this.db = db;
        this.name = name;
        this.usage = usage;
        this.arguments = arguments;
        this.examples = examples;
        this.note = note;
        this.since = since;
        this.deprecated = deprecated;

        // Make the extended description.
        this.extended = arguments + examples;
        if (this.extended.isEmpty()) {
            this.extended = "\n    No example/argument for _FUNC_.\n";
        }
        if (!note.isEmpty()) {
            if (!note.contains("    ") || !note.endsWith("  ")) {
                throw new IllegalArgumentException("'note' is malformed in the expression [" +
                    this.name + "]. It should start with a newline and 4 leading spaces; end " +
                    "with a newline and two spaces; however, got [" + note + "].");
            }
            this.extended += "\n    Note:\n      " + note.trim() + "\n";
        }
        if (!since.isEmpty()) {
            if (Integer.parseInt(since.split("\\.")[0]) < 0) {
                throw new IllegalArgumentException("'since' is malformed in the expression [" +
                    this.name + "]. It should not start with a negative number; however, " +
                    "got [" + since + "].");
            }
            this.extended += "\n    Since: " + since + "\n";
        }
        if (!deprecated.isEmpty()) {
            if (!deprecated.contains("    ") || !deprecated.endsWith("  ")) {
                throw new IllegalArgumentException("'deprecated' is malformed in the " +
                    "expression [" + this.name + "]. It should start with a newline and 4 " +
                    "leading spaces; end with a newline and two spaces; however, got [" +
                    deprecated + "].");
            }
            this.extended += "\n    Deprecated:\n      " + deprecated.trim() + "\n";
        }
    }

    public ExpressionInfo(String className, String name) {
        this(className, null, name, null, "", "", "", "", "");
    }

    public ExpressionInfo(String className, String db, String name) {
        this(className, db, name, null, "", "", "", "", "");
    }

    /**
     * @deprecated This constructor is deprecated as of Spark 3.0. Use other constructors to fully
     *   specify each argument for extended usage.
     */
    @Deprecated
    public ExpressionInfo(String className, String db, String name, String usage, String extended) {
        // `arguments` and `examples` are concatenated for the extended description. So, here
        // simply pass the `extended` as `arguments` and an empty string for `examples`.
        this(className, db, name, usage, extended, "", "", "", "");
    }

    private String replaceFunctionName(String usage) {
        if (usage == null) {
            return "N/A.";
        } else {
            return usage.replaceAll("_FUNC_", name);
        }
    }
}
