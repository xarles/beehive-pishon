package com.kxtx.pishon.core.connector;

import com.kxtx.pishon.core.config.DataTransferConfig;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * Abstract specification of stream data input
 *
 * @author xavier.ma
 *
 */
public abstract class DataSource {

    protected StreamExecutionEnvironment env;

    protected int numPartitions = 1;

    protected long bytes = Long.MAX_VALUE;

    protected String monitorUrls;

    protected List<String> srcCols = new ArrayList<>();


    public List<String> getSrcCols() {
        return srcCols;
    }

    public void setSrcCols(List<String> srcCols) {
        this.srcCols = srcCols;
    }


    protected List<String> jarNameList = new ArrayList<>();

    protected DataSource(DataTransferConfig config, StreamExecutionEnvironment env) {
        this.env = env;
        this.numPartitions = config.getJob().getSetting().getSpeed().getChannel();
        this.bytes = config.getJob().getSetting().getSpeed().getBytes();
        this.monitorUrls = config.getMonitorUrls();
    }

    public abstract DataStream<Row> readData();

    protected DataStream<Row> createInput(InputFormat inputFormat, String sourceName) {
        Preconditions.checkNotNull(sourceName);
        Preconditions.checkNotNull(inputFormat);
        TypeInformation typeInfo = TypeExtractor.getInputFormatTypes(inputFormat);
        InputFormatSourceFunction function = new InputFormatSourceFunction(inputFormat, typeInfo);
        return env.addSource(function, sourceName, typeInfo);
    }
}
