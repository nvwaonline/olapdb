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

import java.io.IOException;
import java.io.StringWriter;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;

public class OlapResultSet extends AvaticaResultSet {

    public OlapResultSet(AvaticaStatement statement, QueryState state, Signature signature, ResultSetMetaData resultSetMetaData, TimeZone timeZone, Frame firstFrame) throws SQLException {
        super(statement, state, signature, resultSetMetaData, timeZone, firstFrame);
    }

    @Override
    protected AvaticaResultSet execute() throws SQLException {

        // skip execution if result is already there (case of meta data lookup)
        if (this.firstFrame != null) {
            return super.execute();
        }

        String sql = signature.sql;
        List<AvaticaParameter> params = signature.parameters;
        List<Object> paramValues = null;
        if (!(statement instanceof OlapPreparedStatement)) {
            params = null;
        } else if (params != null && !params.isEmpty()) {
            paramValues = ((OlapPreparedStatement) statement).getParameterJDBCValues();
        }

        OlapConnection connection = (OlapConnection) statement.connection;
        IRemoteClient client = connection.getRemoteClient();

        Map<String, String> queryToggles = new HashMap<>();
        int maxRows = statement.getMaxRows();
        queryToggles.put("ATTR_STATEMENT_MAX_ROWS", String.valueOf(maxRows));
        addServerProps(queryToggles, connection);

        IRemoteClient.QueryResult result;
        try {
            result = client.executeQuery(sql, paramValues, queryToggles);
        } catch (IOException e) {
            throw new SQLException(e);
        }

        columnMetaDataList.clear();
        columnMetaDataList.addAll(result.columnMeta);

        cursor = MetaImpl.createCursor(signature.cursorFactory, result.iterable);
        return super.execute2(cursor, columnMetaDataList);
    }

    /**
     * add calcite props into queryToggles
     */
    private void addServerProps(Map<String, String> queryToggles, OlapConnection connection) {
        Properties connProps = connection.getConnectionProperties();
        Properties props = new Properties();
        for (String key : connProps.stringPropertyNames()) {
            if (Driver.CLIENT_CALCITE_PROP_NAMES.contains(key)) {
                props.put(key, connProps.getProperty(key));
            }
        }

        if (props.isEmpty()) {
            return;
        }

        StringWriter writer = new StringWriter();
        try {
            props.store(writer, "");
        } catch (IOException ignored) {
            // ignored
            return;
        }
        queryToggles.put("JDBC_CLIENT_CALCITE_PROPS", writer.toString());
    }

}
