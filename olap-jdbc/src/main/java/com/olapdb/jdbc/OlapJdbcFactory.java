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

import org.apache.calcite.avatica.*;
import org.apache.calcite.avatica.Meta.Frame;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.Meta.StatementHandle;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;
import java.util.TimeZone;

public class OlapJdbcFactory implements JdbcFactory {

    public static class Version40 extends OlapJdbcFactory {
        public Version40() {
            super(4, 0);
        }
    }

    public static class Version41 extends OlapJdbcFactory {
        public Version41() {
            super(4, 1);
        }
    }

    final int major;
    final int minor;

    protected OlapJdbcFactory(int major, int minor) {
        this.major = major;
        this.minor = minor;
    }

    @Override
    public int getJdbcMajorVersion() {
        return major;
    }

    @Override
    public int getJdbcMinorVersion() {
        return minor;
    }

    @Override
    public AvaticaConnection newConnection(UnregisteredDriver driver, AvaticaFactory factory, String url, Properties info) throws SQLException {
        return new OlapConnection(driver, (OlapJdbcFactory) factory, url, info);
    }

    @Override
    public AvaticaDatabaseMetaData newDatabaseMetaData(AvaticaConnection connection) {
        return new AvaticaDatabaseMetaData(connection) {
        };
    }

    @Override
    public AvaticaStatement newStatement(AvaticaConnection connection, StatementHandle h, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return new OlapStatement((OlapConnection) connection, h, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public AvaticaPreparedStatement newPreparedStatement(AvaticaConnection connection, StatementHandle h, Signature signature, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return new OlapPreparedStatement((OlapConnection) connection, h, signature, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public AvaticaResultSet newResultSet(AvaticaStatement statement, QueryState state, Signature signature, TimeZone timeZone, Frame firstFrame) throws SQLException {
        AvaticaResultSetMetaData resultSetMetaData = new AvaticaResultSetMetaData(statement, null, signature);
        return new OlapResultSet(statement, state, signature, resultSetMetaData, timeZone, firstFrame);
    }

    @Override
    public ResultSetMetaData newResultSetMetaData(AvaticaStatement statement, Signature signature) throws SQLException {
        return new AvaticaResultSetMetaData(statement, null, signature);
    }

    public IRemoteClient newRemoteClient(OlapConnectionInfo conn) {
        return new OlapClient(conn);
    }
}
