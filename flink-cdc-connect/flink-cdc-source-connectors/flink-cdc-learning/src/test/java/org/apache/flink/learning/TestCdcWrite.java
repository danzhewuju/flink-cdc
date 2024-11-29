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

package org.apache.flink.learning;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @email danyuhao@qq.com
 * @author: Hao Yu
 * @date: 2024/11/28
 * @time: 17:36
 */
public class TestCdcWrite {

    public static void main(String[] args) {
        TestCdcWrite testCdcWrite = new TestCdcWrite();
        System.out.println("开始运行~");
        testCdcWrite.testCdcWrite();
    }

    public void testCdcWrite() {
        String host = "localhost";
        String paimonWarehouse = String.format("hdfs://%s:9000/data/paimon/warehouse", host);

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        configuration.setString("execution.runtime-mode", "streaming");
        // checkpoint 配置
        configuration.setString("execution.checkpointing.mode", "EXACTLY_ONCE");
        configuration.setLong("execution.checkpointing.interval", 30000L);
        configuration.setString("execution.checkpointing.timeout", "10min");
        configuration.setString("execution.checkpointing.min-pause", "10s");
        configuration.setString(
                "execution.checkpointing.externalized-checkpoint-retention",
                "RETAIN_ON_CANCELLATION");
        configuration.setString("execution.checkpointing.tolerable-failed-checkpoints", "100000");
        configuration.setString("execution.checkpointing.max-concurrent-checkpoints", "1");
        configuration.setString(
                "state.checkpoints.dir", String.format("hdfs://%s:9000/flink/checkpoint", host));
        configuration.setString(
                "state.savepoints.dir", String.format("hdfs://%s:9000/flink/savepoint", host));

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(4);
        env.enableCheckpointing(10000L);
        TableEnvironment tableEnv =
                StreamTableEnvironment.create(
                        env, EnvironmentSettings.newInstance().inStreamingMode().build());

        // paimon====================================================================
        tableEnv.executeSql(
                "CREATE TABLE cdc_source (\n"
                        + " id bigint, \n"
                        + " username STRING, \n"
                        + " password STRING, \n"
                        + " email STRING, \n"
                        + " created_at STRING,\n"
                        + "PRIMARY KEY (id) NOT ENFORCED"
                        + ") with ("
                        + "'connector' = 'mysql-cdc', \n"
                        + "'hostname' = '106.55.230.124', \n"
                        + "'port' = '3306',\n"
                        + "'username' = 'yuhao',\n"
                        + "'password' = 'CHINGblxyh666888.mysql',\n"
                        + "'database-name' = 'yuhao_test',\n"
                        + "'table-name' = 'user'\n"
                        + ")\n");

        tableEnv.executeSql(
                "CREATE TABLE log_sink (\n"
                        + " id bigint,\n"
                        + " name STRING,\n"
                        + " password STRING,\n"
                        + " email STRING,\n"
                        + " created_at STRING\n"
                        + ") WITH (\n"
                        + " 'connector' = 'print'\n"
                        + ")");
        tableEnv.executeSql(
                "insert into log_sink select id, username, password, email, cast(created_at as String) from cdc_source ");
    }
}
