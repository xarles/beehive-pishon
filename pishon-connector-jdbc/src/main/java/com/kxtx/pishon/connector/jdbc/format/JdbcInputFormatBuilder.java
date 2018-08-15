package com.kxtx.pishon.connector.jdbc.format;



import com.kxtx.pishon.connector.jdbc.DatabaseInterface;
import com.kxtx.pishon.connector.jdbc.type.TypeConverterInterface;
import com.kxtx.pishon.core.connector.format.RichInputFormatBuilder;

import java.util.List;

/**
 * The builder of JdbcInputFormat
 *
 * @author maxavier
 */



public class JdbcInputFormatBuilder extends RichInputFormatBuilder {

    private JdbcInputFormat format;

    public JdbcInputFormatBuilder() {
        super.format = format = new JdbcInputFormat();
    }

    public void setDrivername(String drivername) {
        format.drivername = drivername;
    }

    public void setDBUrl(String dbURL) {
        format.dbURL = dbURL;
    }

    public void setQuery(String query) {
        format.queryTemplate = query;
    }

    public void setDescColumnTypeList(List<String> columnTypeList) {
        format.descColumnTypeList = columnTypeList;
    }

    public void setParameterValues(Object[][] parameterValues) {
        format.parameterValues = parameterValues;
    }

    public void setUsername(String username) {
        format.username = username;
    }

    public void setPassword(String password) {
        format.password = password;
    }

    public void setTable(String table) {
        format.table = table;
    }

    public void setDatabaseInterface(DatabaseInterface databaseInterface) {
        format.databaseInterface = databaseInterface;
    }

    public void setTypeConverter(TypeConverterInterface converter){
        format.typeConverter = converter;
    }

    public void setColumn(List<String> column){
        format.column = column;
    }

    @Override
    protected void checkFormat() {
        if (format.username == null) {
            LOG.info("Username was not supplied separately.");
        }
        if (format.password == null) {
            LOG.info("Password was not supplied separately.");
        }
        if (format.dbURL == null) {
            throw new IllegalArgumentException("No database URL supplied");
        }
        if (format.queryTemplate == null) {
            throw new IllegalArgumentException("No query supplied");
        }
        if (format.drivername == null) {
            throw new IllegalArgumentException("No driver supplied");
        }
    }

}
