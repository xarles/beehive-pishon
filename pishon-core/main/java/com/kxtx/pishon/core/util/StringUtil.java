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

package com.kxtx.pishon.core.util;

import com.dtstack.flinkx.common.ColumnType;
import com.dtstack.flinkx.exception.WriteRecordException;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.math.BigDecimal;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.dtstack.flinkx.common.ColumnType.valueOf;

/**
 * String Utilities
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class StringUtil {

    /**
     * Handle the escaped escape charactor.
     *
     * e.g. Turnning \\t into \t, etc.
     *
     * @param str The String to convert
     * @return the converted String
     */
    public static String convertRegularExpr (String str) {
        String pattern = "\\\\(\\d{3})";

        Pattern r = Pattern.compile(pattern);
        while(true) {
            Matcher m = r.matcher(str);
            if(!m.find()) {
                break;
            }
            String num = m.group(1);
            int x = Integer.parseInt(num, 8);
            str = m.replaceFirst(String.valueOf((char)x));
        }
        str = str.replaceAll("\\\\t","\t");
        str = str.replaceAll("\\\\r","\r");
        str = str.replaceAll("\\\\n","\n");

        return str;
    }

    public static Object string2col(String str, String type) {

        Preconditions.checkNotNull(type);
        ColumnType columnType = valueOf(type.toUpperCase());
        Object ret;
        switch(columnType) {
            case TINYINT:
                ret = Byte.valueOf(str);
                break;
            case SMALLINT:
                ret = Short.valueOf(str);
                break;
            case INT:
                ret = Integer.valueOf(str);
                break;
            case MEDIUMINT:
            case BIGINT:
                ret = Long.valueOf(str);
                break;
            case FLOAT:
                ret = Float.valueOf(str);
                break;
            case DOUBLE:
                ret = Double.valueOf(str);
                break;
            case STRING:
            case VARCHAR:
            case CHAR:
                ret = str;
                break;
            case BOOLEAN:
                ret = Boolean.valueOf(str.toLowerCase());
                break;
            case DATE:
                ret = DateUtil.columnToDate(str);
                break;
            case TIMESTAMP:
            case DATETIME:
                ret = DateUtil.columnToTimestamp(str);
                break;
            default:
                throw new IllegalArgumentException();
        }

        return ret;
    }

    public static String col2string(Object column, String type) {
        String rowData = column.toString();
        ColumnType columnType = ColumnType.valueOf(type.toUpperCase());
        Object result = null;
        switch (columnType) {
            case TINYINT:
                result = Byte.valueOf(rowData);
                break;
            case SMALLINT:
                result = Short.valueOf(rowData);
                break;
            case INT:
                result = Integer.valueOf(rowData);
                break;
            case BIGINT:
                result = Long.valueOf(rowData);
                break;
            case FLOAT:
                result = Float.valueOf(rowData);
                break;
            case DOUBLE:
                result = Double.valueOf(rowData);
                break;
            case DECIMAL:
                result = new BigDecimal(rowData);
                break;
            case STRING:
            case VARCHAR:
            case CHAR:
                result = rowData;
                break;
            case BOOLEAN:
                result = Boolean.valueOf(rowData);
                break;
            case DATE:
                result = DateUtil.dateToString((java.util.Date)column);
                break;
            case TIMESTAMP:
                result = DateUtil.timestampToString((java.util.Date)column);
                break;
            default:
                throw new IllegalArgumentException();
        }
        return result.toString();
    }


    public static String row2string(Row row, List<String> columnTypes, String delimiter, List<String> columnNames) throws WriteRecordException {
        // convert row to string
        int cnt = row.getArity();
        StringBuilder sb = new StringBuilder();

        int i = 0;
        try {
            for (; i < cnt; ++i) {
                if (i != 0) {
                    sb.append(delimiter);
                }

                Object column = row.getField(i);

                if(column == null) {
                    continue;
                }

                sb.append(col2string(column, columnTypes.get(i)));
            }
        } catch(Exception ex) {
            String msg = "StringUtil.row2string error: when converting field[" + i + "] in Row(" + row + ")";
            throw new WriteRecordException(msg, ex, i, row);
        }

        return sb.toString();
    }

}
