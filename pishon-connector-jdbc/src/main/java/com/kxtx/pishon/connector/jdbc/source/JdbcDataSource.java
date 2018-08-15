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

package com.kxtx.pishon.connector.jdbc.source;


import com.kxtx.pishon.core.config.DataTransferConfig;
import com.kxtx.pishon.core.config.ReaderConfig;
import com.kxtx.pishon.connector.jdbc.DatabaseInterface;
import com.kxtx.pishon.connector.jdbc.format.JdbcInputFormatBuilder;
import com.kxtx.pishon.connector.jdbc.type.TypeConverterInterface;
import com.kxtx.pishon.connector.jdbc.util.DBUtil;
import com.kxtx.pishon.core.connector.DataSource;
import com.kxtx.pishon.core.util.ClassUtil;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.api.java.io.jdbc.split.ParameterValuesProvider;

import java.io.Serializable;
import java.sql.*;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The Reader plugin for any database that can be connected via JDBC.
 *
 *  * @author maxavier
 */
public class JdbcDataSource extends DataSource {

    protected DatabaseInterface databaseInterface;

    protected TypeConverterInterface typeConverter;

    protected String dbUrl;

    protected String username;

    protected String password;

    protected List<String> column;

    protected String[] columnTypes;

    protected String table;

    protected Connection connection;

    protected String where;

    protected String splitKey;

    public void setDatabaseInterface(DatabaseInterface databaseInterface) {
        this.databaseInterface = databaseInterface;
    }

    public void setTypeConverterInterface(TypeConverterInterface typeConverter) {
        this.typeConverter = typeConverter;
    }

    public JdbcDataSource(DataTransferConfig config, StreamExecutionEnvironment env) {

        super(config, env);

        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        dbUrl = readerConfig.getParameter().getConnection().get(0).getJdbcUrl().get(0);

        if(readerConfig.getName().equalsIgnoreCase("mysqlreader")) {
            String[] splits = dbUrl.split("\\?");

            Map<String,String> paramMap = new HashMap<String,String>();
            if(splits.length > 1) {
                String[] pairs = splits[1].split("&");
                for(String pair : pairs) {
                    String[] leftRight = pair.split("=");
                    paramMap.put(leftRight[0], leftRight[1]);
                }
            }
            paramMap.put("useCursorFetch", "true");


            StringBuffer sb = new StringBuffer(splits[0]);
            if(paramMap.size() != 0) {
                sb.append("?");
                int index = 0;
                for(Map.Entry<String,String> entry : paramMap.entrySet()) {
                    if(index != 0) {
                        sb.append("&");
                    }
                    sb.append(entry.getKey() + "=" + entry.getValue());
                    index++;
                }
            }

            dbUrl = sb.toString();
        }
        username = readerConfig.getParameter().getStringVal(JdbcConfigKeys.KEY_USER_NAME);
        password = readerConfig.getParameter().getStringVal(JdbcConfigKeys.KEY_PASSWORD);
        table = readerConfig.getParameter().getConnection().get(0).getTable().get(0);
        where = readerConfig.getParameter().getStringVal(JdbcConfigKeys.KEY_WHERE);
        column = readerConfig.getParameter().getColumn();
        splitKey = readerConfig.getParameter().getStringVal(JdbcConfigKeys.KEY_SPLIK_KEY);

    }

    @Override
    public DataStream<Row> readData() {
        // Read from JDBC
        JdbcInputFormatBuilder builder = new JdbcInputFormatBuilder();
        builder.setDrivername(databaseInterface.getDriverClass());
        builder.setDBUrl(dbUrl);
        builder.setQuery(getQuerySql());
        builder.setUsername(username);
        builder.setPassword(password);
        builder.setBytes(bytes);
        builder.setMonitorUrls(monitorUrls);
        builder.setDescColumnTypeList(descColumnTypes());
        builder.setTable(table);
        builder.setDatabaseInterface(databaseInterface);
        builder.setTypeConverter(typeConverter);
        builder.setColumn(column);

        if(numPartitions > 1 && splitKey != null && splitKey.trim().length() != 0) {
            final int channels = numPartitions;
            ParameterValuesProvider provider = new ParameterValuesProvider() {
                @Override
                public Serializable[][] getParameterValues() {
                    Integer[][] parameters = new Integer[channels][];
                    for(int i = 0; i < channels; ++i) {
                        parameters[i] = new Integer[2];
                        parameters[i][0] = channels;
                        parameters[i][1] = i;
                    }
                    return parameters;
                }
            };

            builder.setParameterValues(provider.getParameterValues());

        }

        RichInputFormat format =  builder.finish();
        return createInput(format, (databaseInterface.getDatabaseType() + "reader").toLowerCase());

    }


    /**
     * FIXME 不通过该方法获取表的字段属性
     * 暂时保留
     * @return
     */
    private RowTypeInfo getColumnTypes() {

        String sql = databaseInterface.getSQLQueryColumnFields(column, table);

        try (Connection conn = getConnection()) {
            Statement stmt = conn.createStatement();
            ResultSetMetaData rsmd = stmt.executeQuery(sql).getMetaData();
            int cnt = rsmd.getColumnCount();
            TypeInformation[] typeInformations = new TypeInformation[cnt];
            for(int i = 0; i < cnt; ++i) {
                Class<?> clz = Class.forName(rsmd.getColumnClassName(i + 1));
                if(clz == Timestamp.class) {
                    clz = java.util.Date.class;
                }
                typeInformations[i] = BasicTypeInfo.getInfoFor(clz);
            }
            return new RowTypeInfo(typeInformations);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

    }

    protected RowTypeInfo getColumnTypesBySetting() {
        try{
            int cnt = columnTypes.length;
            TypeInformation[] typeInformations = new TypeInformation[cnt];
            for(int i = 0; i < cnt; ++i) {
                Class<?> clz = Class.forName(columnTypes[i]);
                if(clz == Timestamp.class || clz == Date.class || clz == Time.class || columnTypes[i].toUpperCase().contains("TIMESTAMP")) {
                    clz = java.util.Date.class;
                }
                typeInformations[i] = BasicTypeInfo.getInfoFor(clz);
            }
            return new RowTypeInfo(typeInformations);
        }catch (Exception e){
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }


    /**
     * Build query sql including needed columns
     * query filters and partitioning filters
     * @return sql
     */
    protected String getQuerySql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ").append(databaseInterface.quoteColumns(column)).append(" FROM ");
        sb.append(databaseInterface.quoteTable(table));

        StringBuilder filter = new StringBuilder();

        if(numPartitions != 1 && splitKey != null && splitKey.trim().length() != 0) {
            filter.append(databaseInterface.getSplitFilter(splitKey));
        }

        if(where != null && where.trim().length() != 0) {
            if(filter.length() > 0) {
                filter.append(" AND ");
            }
            filter.append(where);
        }

        if(filter.length() != 0) {
            sb.append(" WHERE ").append(filter);
        }

        System.out.println(sb);
        return sb.toString();
    }

    protected Connection getConnection() {
        try {
            ClassUtil.forName(databaseInterface.getDriverClass(), this.getClass().getClassLoader());
            connection = DBUtil.getConnection(dbUrl, username, password);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }

        return connection;

    }

    protected List<String> descColumnTypes() {
        return null;
    }

}
