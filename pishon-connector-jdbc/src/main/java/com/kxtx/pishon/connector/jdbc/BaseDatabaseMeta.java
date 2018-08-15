/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kxtx.pishon.connector.jdbc;

import org.apache.commons.lang.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Abstract base parent class of other database prototype implementations
 *
 * @author maxavier
 */
public abstract class BaseDatabaseMeta implements DatabaseInterface, Serializable {

    @Override
    public String getStartQuote() {
        return "\"";
    }

    @Override
    public String getEndQuote() {
        return "\"";
    }

    @Override
    public String quoteColumn(String column) {
        return getStartQuote() + column + getEndQuote();
    }

    @Override
    public String quoteColumns(List<String> column) {
        return quoteColumns(column, null);
    }

    @Override
    public String quoteColumns(List<String> column, String table) {
        String prefix = StringUtils.isBlank(table) ? "" : quoteTable(table) + ".";
        List<String> list = new ArrayList<>();
        for(String col : column) {
            list.add(prefix + quoteColumn(col));
        }
        return StringUtils.join(list, ",");
    }

    @Override
    public String quoteTable(String table) {
        String[] parts = table.split("\\.");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.length; ++i) {
            if(i != 0) {
                sb.append(".");
            }
            sb.append(getStartQuote() + parts[i] + getEndQuote());
        }
        return sb.toString();
    }

    protected List<String> keyColList(Map<String,List<String>> updateKey) {
        List<String> keyCols = new ArrayList<>();
        for(Map.Entry<String,List<String>> entry : updateKey.entrySet()) {
            List<String> list = entry.getValue();
            for(String col : list) {
                if(!keyCols.contains(col)) {
                    keyCols.add(col);
                }
            }
        }
        return keyCols;
    }

    @Override
    public String getReplaceStatement(List<String> column, List<String> fullColumn, String table, Map<String,List<String>> updateKey) {
        if(updateKey == null || updateKey.isEmpty()) {
            return getInsertStatement(column, table);
        }

        return "MERGE INTO " + quoteTable(table) + " T1 USING "
                + "(" + makeValues(column) + ") T2 ON ("
                + updateKeySql(updateKey) + ") WHEN MATCHED THEN UPDATE SET "
                + getUpdateSql(column, fullColumn, "T1", "T2", keyColList(updateKey)) + " WHEN NOT MATCHED THEN "
                + "INSERT (" + quoteColumns(column) + ") VALUES ("
                + quoteColumns(column, "T2") + ")";
    }

    @Override
    public String getUpsertStatement(List<String> column, String table, Map<String,List<String>> updateKey) {
        if(updateKey == null || updateKey.isEmpty()) {
            return getInsertStatement(column, table);
        }
        return getReplaceStatement(column, column, table, updateKey);
    }

    abstract protected String makeMultipleValues(int nCols, int batchSize);

    protected String makeMultipleValues(List<String> column, int batchSize) {
        String value = makeValues(column);
        return StringUtils.repeat(value, " UNION ALL ", batchSize);
    }

    @Override
    public String getMultiInsertStatement(List<String> column, String table, int batchSize) {
        return "INSERT INTO " + quoteTable(table)
                + " (" + quoteColumns(column) + ") "
                + makeMultipleValues(column.size(), batchSize);
    }

    abstract protected String makeValues(int nCols);

    abstract protected String makeValues(List<String> column);

    @Override
    public String getMultiReplaceStatement(List<String> column, List<String> fullColumn, String table, int batchSize, Map<String,List<String>> updateKey) {
        if(updateKey == null || updateKey.isEmpty()) {
            return getMultiInsertStatement(column, table, batchSize);
        }

        return "MERGE INTO " + quoteTable(table) + " T1 USING "
                + "(" + makeMultipleValues(column, batchSize) + ") T2 ON ("
                + updateKeySql(updateKey) + ") WHEN MATCHED THEN UPDATE SET "
                + getUpdateSql(column, fullColumn, "T1", "T2", keyColList(updateKey)) + " WHEN NOT MATCHED THEN "
                + "INSERT (" + quoteColumns(column) + ") VALUES ("
                + quoteColumns(column, "T2") + ")";
    }

    @Override
    public String getMultiUpsertStatement(List<String> column, String table, int batchSize, Map<String,List<String>> updateKey) {
        if(updateKey == null || updateKey.isEmpty()) {
            return getMultiInsertStatement(column, table, batchSize);
        }

        return getMultiReplaceStatement(column, column, table, batchSize, updateKey);
    }

    protected String getUpdateSql(List<String> column, String leftTable, String rightTable) {
        String prefixLeft = StringUtils.isBlank(leftTable) ? "" : quoteTable(leftTable) + ".";
        String prefixRight = StringUtils.isBlank(rightTable) ? "" : quoteTable(rightTable) + ".";
        List<String> list = new ArrayList<>();
        for(String col : column) {
            list.add(prefixLeft + leftTable + col + "=" + prefixRight + rightTable + col);
        }
        return StringUtils.join(list, ",");
    }

    protected String getUpdateSql(List<String> column, List<String> fullColumn, String leftTable, String rightTable, List<String> keyCols) {
        String prefixLeft = StringUtils.isBlank(leftTable) ? "" : quoteTable(leftTable) + ".";
        String prefixRight = StringUtils.isBlank(rightTable) ? "" : quoteTable(rightTable) + ".";
        List<String> list = new ArrayList<>();
        for(String col : fullColumn) {
            if(keyCols == null || keyCols.size() == 0 ||  keyCols.contains(col)) {
                continue;
            }
            if(fullColumn == null || column.contains(col)) {
                list.add(prefixLeft + col + "=" + prefixRight + col);
            } else {
                list.add(prefixLeft + col + "=null");
            }
        }
        return StringUtils.join(list, ",");
    }

    protected String updateKeySql(Map<String,List<String>> updateKey) {
        List<String> exprList = new ArrayList<>();
        for(Map.Entry<String,List<String>> entry : updateKey.entrySet()) {
            List<String> colList = new ArrayList<>();
            for(String col : entry.getValue()) {
                colList.add("T1." + quoteColumn(col) + "=T2." + quoteColumn(col));
            }
            exprList.add(StringUtils.join(colList, " AND "));
        }
        return StringUtils.join(exprList, " OR ");
    }

    @Override
    public String getInsertStatement(List<String> column, String table) {
        return "INSERT INTO " + quoteTable(table)
                + " (" + quoteColumns(column) + ") values ("
                + StringUtils.repeat("?", ",", column.size()) + ")";
    }

}
