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

package com.kxtx.pishon.connector.jdbc.format;

import com.kxtx.pishon.connector.jdbc.DatabaseInterface;
import com.kxtx.pishon.connector.jdbc.type.TypeConverterInterface;
import com.kxtx.pishon.connector.jdbc.util.DBUtil;
import com.kxtx.pishon.core.connector.format.RichInputFormat;
import com.kxtx.pishon.core.util.ClassUtil;
import com.kxtx.pishon.core.util.DateUtil;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.util.*;

/**
 * InputFormat for reading data from a database and generate Rows.
 *
 * @author maxavier
 */
public class JdbcInputFormat extends RichInputFormat {

    protected static final long serialVersionUID = 1L;

    protected DatabaseInterface databaseInterface;

    protected String username;

    protected String password;

    protected String drivername;

    protected String dbURL;

    protected String queryTemplate;

    protected int resultSetType;

    protected int resultSetConcurrency;

    protected List<String> descColumnTypeList;

    protected transient Connection dbConn;

    protected transient PreparedStatement statement;

    protected transient ResultSet resultSet;

    protected boolean hasNext;

    protected Object[][] parameterValues;

    protected int columnCount;

    protected String table;

    protected TypeConverterInterface typeConverter;

    protected List<String> column;

    public JdbcInputFormat() {
        resultSetType = ResultSet.TYPE_FORWARD_ONLY;
        resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public void configure(Configuration configuration) {

    }

    private List<String> analyzeTable() {
        List<String> ret = new ArrayList<>();

        try {
            Statement stmt = dbConn.createStatement();
            ResultSet rs = stmt.executeQuery(databaseInterface.getSQLQueryFields(databaseInterface.quoteTable(table)));
            ResultSetMetaData rd = rs.getMetaData();

            Map<String,String> nameTypeMap = new HashMap<>();
            for(int i = 0; i < rd.getColumnCount(); ++i) {
                nameTypeMap.put(rd.getColumnName(i+1),rd.getColumnTypeName(i+1));
            }

            for (String col : column) {
                ret.add(nameTypeMap.get(col));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return ret;
    }

    @Override
    public void openInternal(InputSplit inputSplit) throws IOException {
        try {
            ClassUtil.forName(drivername, getClass().getClassLoader());
            dbConn = DBUtil.getConnection(dbURL, username, password);
            statement = dbConn.prepareStatement(queryTemplate, resultSetType, resultSetConcurrency);

            if (inputSplit != null && parameterValues != null) {
                for (int i = 0; i < parameterValues[inputSplit.getSplitNumber()].length; i++) {
                    Object param = parameterValues[inputSplit.getSplitNumber()][i];
                    if (param instanceof String) {
                        statement.setString(i + 1, (String) param);
                    } else if (param instanceof Long) {
                        statement.setLong(i + 1, (Long) param);
                    } else if (param instanceof Integer) {
                        statement.setInt(i + 1, (Integer) param);
                    } else if (param instanceof Double) {
                        statement.setDouble(i + 1, (Double) param);
                    } else if (param instanceof Boolean) {
                        statement.setBoolean(i + 1, (Boolean) param);
                    } else if (param instanceof Float) {
                        statement.setFloat(i + 1, (Float) param);
                    } else if (param instanceof BigDecimal) {
                        statement.setBigDecimal(i + 1, (BigDecimal) param);
                    } else if (param instanceof Byte) {
                        statement.setByte(i + 1, (Byte) param);
                    } else if (param instanceof Short) {
                        statement.setShort(i + 1, (Short) param);
                    } else if (param instanceof Date) {
                        statement.setDate(i + 1, (Date) param);
                    } else if (param instanceof Time) {
                        statement.setTime(i + 1, (Time) param);
                    } else if (param instanceof Timestamp) {
                        statement.setTimestamp(i + 1, (Timestamp) param);
                    } else if (param instanceof Array) {
                        statement.setArray(i + 1, (Array) param);
                    } else {
                        //extends with other types if needed
                        throw new IllegalArgumentException("open() failed. Parameter " + i + " of type " + param.getClass() + " is not handled (yet)." );
                    }
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format("Executing '%s' with parameters %s", queryTemplate, Arrays.deepToString(parameterValues[inputSplit.getSplitNumber()])));
                }
            }
            statement.setFetchSize(1000);
            statement.setQueryTimeout(1000);
            resultSet = statement.executeQuery();
            hasNext = resultSet.next();
            columnCount = resultSet.getMetaData().getColumnCount();

            if(descColumnTypeList == null) {
                descColumnTypeList = analyzeTable();
            }
        } catch (SQLException se) {
            throw new IllegalArgumentException("open() failed." + se.getMessage(), se);
        }

        LOG.info("JdbcInputFormat[" + jobName + "]open: end");
    }


    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
        return cachedStatistics;
    }

    @Override
    public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
        if (parameterValues == null) {
            return new GenericInputSplit[]{new GenericInputSplit(0, 1)};
        }
        GenericInputSplit[] ret = new GenericInputSplit[parameterValues.length];
        for (int i = 0; i < ret.length; i++) {
            ret[i] = new GenericInputSplit(i, ret.length);
        }
        return ret;
    }


    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }


    @Override
    public boolean reachedEnd() throws IOException {
        return !hasNext;
    }

    @Override
    public Row nextRecordInternal(Row row) throws IOException {
        row = new Row(columnCount);
        try {
            if (!hasNext) {
                return null;
            }

            for (int pos = 0; pos < row.getArity(); pos++) {
                Object obj = resultSet.getObject(pos + 1);
                if(obj != null) {
                    if (dbURL.startsWith("jdbc:oracle")) {
                        if((obj instanceof java.util.Date || obj.getClass().getSimpleName().toUpperCase().contains("TIMESTAMP")) ) {
                            obj = resultSet.getTimestamp(pos + 1);
                        }
                    } else if(dbURL.startsWith("jdbc:mysql")) {
                        if(descColumnTypeList != null && descColumnTypeList.size() != 0) {
                            if(descColumnTypeList.get(pos).equalsIgnoreCase("year")) {
                                java.util.Date date = (java.util.Date) obj;
                                String year = DateUtil.dateToYearString(date);
                                System.out.println(year);
                                obj = year;
                            } else if(descColumnTypeList.get(pos).equalsIgnoreCase("tinyint")) {
                                if(obj instanceof Boolean) {
                                    obj = ((Boolean) obj ? 1 : 0);
                                }
                            } else if(descColumnTypeList.get(pos).equalsIgnoreCase("bit")) {
                                if(obj instanceof Boolean) {
                                    obj = ((Boolean) obj ? 1 : 0);
                                }
                            }
                        }
                    } else if(dbURL.startsWith("jdbc:sqlserver")) {
                        if(descColumnTypeList != null && descColumnTypeList.size() != 0) {
                            if(descColumnTypeList.get(pos).equalsIgnoreCase("bit")) {
                                if(obj instanceof Boolean) {
                                    obj = ((Boolean) obj ? 1 : 0);
                                }
                            }
                        }
                    } else if(dbURL.startsWith("jdbc:postgresql")){
                        if(descColumnTypeList != null && descColumnTypeList.size() != 0) {
                            obj = typeConverter.convert(obj,descColumnTypeList.get(pos));
                        }
                    }
                }

                row.setField(pos, obj);
            }

            //update hasNext after we've read the record
            hasNext = resultSet.next();
            return row;
        } catch (SQLException se) {
            throw new IOException("Couldn't read data - " + se.getMessage(), se);
        } catch (NullPointerException npe) {
            throw new IOException("Couldn't access resultSet", npe);
        }
    }

    @Override
    public void closeInternal() throws IOException {
        DBUtil.closeDBResources(resultSet,statement,dbConn);
        parameterValues = null;
    }

}
