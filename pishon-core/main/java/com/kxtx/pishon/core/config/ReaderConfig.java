/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kxtx.pishon.core.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The configuration of Reader
 *
 * @author maxavier
 */
public class ReaderConfig extends AbstractConfig {

    public static String KEY_PARAMETER_CONFIG = "parameter";

    public static final String KEY_READER_NAME = "name";

    private ParameterConfig parameter;

    public ReaderConfig(Map<String, Object> map) {
        super(map);
        parameter = new ParameterConfig((Map<String, Object>) getVal(KEY_PARAMETER_CONFIG));
    }

    public String getName() {
        return getStringVal(KEY_READER_NAME);
    }

    public void setName(String name) {
        setStringVal(KEY_READER_NAME, name);
    }

    public ParameterConfig getParameter() {
        return parameter;
    }

    public void setParameter(ParameterConfig parameter) {
        this.parameter = parameter;
    }

    public static class ParameterConfig extends AbstractConfig {
        public static final String KEY_COLUMN_LIST = "column";
        public static final String KEY_CONNECTION_CONFIG_LIST = "connection";

        List column;
        List<ConnectionConfig> connection;

        public ParameterConfig(Map<String, Object> map) {
            super(map);
            column = (List) getVal(KEY_COLUMN_LIST);
            List<Map<String,Object>> connList = (List<Map<String, Object>>) getVal(KEY_CONNECTION_CONFIG_LIST);
            connection = new ArrayList<>();
            if(connList != null) {
                for(Map<String,Object> conn : connList) {
                    connection.add(new ParameterConfig.ConnectionConfig(conn));
                }
            }
        }

        public List<ConnectionConfig> getConnection() {
            return connection;
        }

        public void setConnection(List<ConnectionConfig> connection) {
            this.connection = connection;
        }

        public List getColumn() {
            return column;
        }

        public void setColumn(List column) {
            this.column = column;
        }

        public class ConnectionConfig extends AbstractConfig {

            public static final String KEY_TABLE_LIST = "table";
            public static final String KEY_JDBC_URL_LIST = "jdbcUrl";

            public ConnectionConfig(Map<String, Object> map) {
                super(map);
            }

            public List<String> getTable() {
                return (List<String>) getVal(KEY_TABLE_LIST);
            }

            public void setTable(List<String> table) {
                setVal(KEY_TABLE_LIST, table);
            }

            public List<String> getJdbcUrl() {
                return (List<String>) getVal(KEY_JDBC_URL_LIST);
            }

            public void setJdbcUrl(List<String> jdbcUrl) {
                setVal(KEY_JDBC_URL_LIST, jdbcUrl);
            }
        }

    }
}
