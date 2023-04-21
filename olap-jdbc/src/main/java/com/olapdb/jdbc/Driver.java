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

package com.olapdb.jdbc;

import com.google.common.collect.Sets;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.UnregisteredDriver;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Set;

public class Driver extends UnregisteredDriver {

    public static final String CONNECT_STRING_PREFIX = "jdbc:olap:";

    /**
     * These calcite props can be configured by jdbc connection
     */
    protected static final Set<String> CLIENT_CALCITE_PROP_NAMES = Sets.newHashSet(
            "caseSensitive",
            "unquotedCasing",
            "quoting",
            "conformance"
    );

    static {
        try {
            DriverManager.registerDriver(new Driver());
        } catch (SQLException e) {
            throw new RuntimeException("Error occurred while registering JDBC driver " + Driver.class.getName() + ": " + e.toString());
        }
    }

    @Override
    protected String getConnectStringPrefix() {
        return CONNECT_STRING_PREFIX;
    }

    @Override
    protected DriverVersion createDriverVersion() {
        return DriverVersion.load(Driver.class, "olap-jdbc.properties", "Olap JDBC Driver", "unknown version", "Olap JDBC Connector", "unknown version");
    }

    @Override
    protected String getFactoryClassName(JdbcVersion jdbcVersion) {
        switch (jdbcVersion) {
        case JDBC_30:
            throw new UnsupportedOperationException();
        case JDBC_40:
            return OlapJdbcFactory.Version40.class.getName();
        case JDBC_41:
        default:
            return OlapJdbcFactory.Version41.class.getName();
        }
    }

    @Override
    public Meta createMeta(AvaticaConnection connection) {
        return new OlapMeta((OlapConnection) connection);
    }

}
