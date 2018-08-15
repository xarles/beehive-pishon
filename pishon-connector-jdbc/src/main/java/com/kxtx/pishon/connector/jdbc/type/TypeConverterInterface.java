package com.kxtx.pishon.connector.jdbc.type;

import java.io.Serializable;

/**
 * @author jiangbo
 */
public interface TypeConverterInterface extends Serializable {

    Object convert(Object data,String typeName);

}
