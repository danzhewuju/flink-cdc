/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.mysql.testutils;

/**
 * A {@link MySqlConnectionProvider} backed by a remote MySQL server. Connection parameters are read
 * from system properties:
 *
 * <ul>
 *   <li>{@code mysql.remote.host} - MySQL server hostname or IP (required)
 *   <li>{@code mysql.remote.port} - MySQL server port (default 3306)
 *   <li>{@code mysql.remote.user} - MySQL username (required)
 *   <li>{@code mysql.remote.password} - MySQL password (required)
 * </ul>
 *
 * <p>Usage: pass {@code -Dmysql.remote.host=xxx -Dmysql.remote.user=xxx -Dmysql.remote.password=xxx}
 * as JVM arguments when running tests.
 */
public class RemoteMySqlServer implements MySqlConnectionProvider {

    public static final String PROP_HOST = "mysql.remote.host";
    public static final String PROP_PORT = "mysql.remote.port";
    public static final String PROP_USER = "mysql.remote.user";
    public static final String PROP_PASSWORD = "mysql.remote.password";

    private final String host;
    private final int port;
    private final String username;
    private final String password;

    public RemoteMySqlServer() {
        this.host = requireProperty(PROP_HOST);
        this.port = Integer.parseInt(System.getProperty(PROP_PORT, "3306"));
        this.username = requireProperty(PROP_USER);
        this.password = requireProperty(PROP_PASSWORD);
    }

    public RemoteMySqlServer(String host, int port, String username, String password) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
    }

    /** Returns true if the system property {@code mysql.remote.host} is set. */
    public static boolean isRemoteEnabled() {
        String host = System.getProperty(PROP_HOST);
        return host != null && !host.isEmpty();
    }

    @Override
    public String getHost() {
        return host;
    }

    @Override
    public int getDatabasePort() {
        return port;
    }

    @Override
    public String getUsername() {
        return username;
    }

    @Override
    public String getPassword() {
        return password;
    }

    @Override
    public String getJdbcUrl() {
        return "jdbc:mysql://" + host + ":" + port + "?useSSL=false&allowPublicKeyRetrieval=true";
    }

    @Override
    public String getJdbcUrl(String databaseName) {
        return "jdbc:mysql://"
                + host
                + ":"
                + port
                + "/"
                + databaseName
                + "?useSSL=false&allowPublicKeyRetrieval=true";
    }

    @Override
    public void stop() {
        // no-op: we don't manage the remote server's lifecycle
    }

    private static String requireProperty(String key) {
        String value = System.getProperty(key);
        if (value == null || value.isEmpty()) {
            throw new IllegalStateException(
                    "System property '" + key + "' is required for remote MySQL connection");
        }
        return value;
    }
}
