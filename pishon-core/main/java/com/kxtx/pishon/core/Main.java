package com.kxtx.pishon.core;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Main {

    public static int Pishon_Env_Parallelism = 2;




    public static  void main(String[] args) throws Exception {

        //构造并执行flink任务
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(Pishon_Env_Parallelism);
        env.setRestartStrategy(RestartStrategies.noRestart());
//        env.addSource()

        env.execute("");

    }

}
