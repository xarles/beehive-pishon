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

package com.kxtx.pishon.core.connector;

import com.kxtx.pishon.core.config.DataTransferConfig;
import com.kxtx.pishon.core.config.ReaderConfig;
import com.kxtx.pishon.core.plugin.PluginLoader;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.lang.reflect.Constructor;

/**
 * The factory of DataReader
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class DataSourceFactory {

    private DataSourceFactory() {
    }


    public static DataSource getDataSource(DataTransferConfig config, StreamExecutionEnvironment env) {

        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();

        PluginLoader pluginLoader = new PluginLoader(readerConfig.getName().toLowerCase(), config.getPluginRoot());
        Class<?> clz = pluginLoader.getPluginClass();

        try {
            Constructor constructor = clz.getConstructor(DataTransferConfig.class, StreamExecutionEnvironment.class);
            return (DataSource) constructor.newInstance(config, env);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}