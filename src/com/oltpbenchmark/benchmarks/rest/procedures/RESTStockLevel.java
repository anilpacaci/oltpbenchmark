/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/

package com.oltpbenchmark.benchmarks.rest.procedures;

import java.sql.SQLException;
import java.util.Random;

import javax.ws.rs.core.MediaType;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;

import com.oltpbenchmark.benchmarks.rest.RESTWorker;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource.Builder;

public class RESTStockLevel extends RESTProcedure {

    private static final Logger LOG = Logger.getLogger(RESTStockLevel.class);

    private Builder builder;

    private static String SQL_VARIABLE = "?";

    private String stockGetDistOrderIdSQLWithVariables = "SELECT D_NEXT_O_ID, D_NEXT_O_ID - 20, D_W_ID, D_ID, D_W_ID, CAST(FLOOR( 11 + RAND() * 10000) as UNSIGNED) as THRESHOLD FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?";

    private String stockGetCountStockSQLWithVariables = "SELECT COUNT(DISTINCT (S_I_ID)) AS STOCK_COUNT" + " FROM " + TPCCConstants.TABLENAME_ORDERLINE + ", " + TPCCConstants.TABLENAME_STOCK
            + " WHERE OL_W_ID = ?" + " AND OL_D_ID = ?" + " AND OL_O_ID < ?" + " AND OL_O_ID >= ?" + " AND S_W_ID = ?" + " AND S_I_ID = OL_I_ID" + " AND S_QUANTITY < ?";

    private String stockGetCountStockSQLWithVariables2 = "SELECT S_W_ID, S_I_ID" + " FROM " + TPCCConstants.TABLENAME_ORDERLINE + ", " + TPCCConstants.TABLENAME_STOCK + " WHERE OL_W_ID = ?"
            + " AND OL_D_ID = ?" + " AND OL_O_ID < ?" + " AND OL_O_ID >= ?" + " AND S_W_ID = ?" + " AND S_I_ID = OL_I_ID" + " AND S_QUANTITY < ?";

    private String stockGetCountStockSQLWithVariables3 = "SELECT S_W_ID, S_I_ID, S_QUANTITY" + " FROM " + TPCCConstants.TABLENAME_ORDERLINE + ", " + TPCCConstants.TABLENAME_STOCK
            + " WHERE OL_W_ID = ?" + " AND OL_D_ID = ?" + " AND OL_O_ID < ?" + " AND OL_O_ID >= ?" + " AND S_W_ID = ?" + " AND S_I_ID = OL_I_ID" + " AND S_QUANTITY < ?";

    private String stockGetCountStockSQLWithVariables4 = "SELECT S_W_ID, S_I_ID, S_QUANTITY, S_YTD" + " FROM " + TPCCConstants.TABLENAME_ORDERLINE + ", " + TPCCConstants.TABLENAME_STOCK
            + " WHERE OL_W_ID = ?" + " AND OL_D_ID = ?" + " AND OL_O_ID < ?" + " AND OL_O_ID >= ?" + " AND S_W_ID = ?" + " AND S_I_ID = OL_I_ID" + " AND S_QUANTITY < ?";

    private String stockGetCountStockSQLWithVariables5 = "SELECT S_W_ID, S_I_ID, S_QUANTITY, S_YTD, S_ORDER_CNT" + " FROM " + TPCCConstants.TABLENAME_ORDERLINE + ", " + TPCCConstants.TABLENAME_STOCK
            + " WHERE OL_W_ID = ?" + " AND OL_D_ID = ?" + " AND OL_O_ID < ?" + " AND OL_O_ID >= ?" + " AND S_W_ID = ?" + " AND S_I_ID = OL_I_ID" + " AND S_QUANTITY < ?";

    private String stockGetCountStockSQLWithVariables6 = "SELECT S_W_ID, S_I_ID, S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT" + " FROM " + TPCCConstants.TABLENAME_ORDERLINE + ", "
            + TPCCConstants.TABLENAME_STOCK + " WHERE OL_W_ID = ?" + " AND OL_D_ID = ?" + " AND OL_O_ID < ?" + " AND OL_O_ID >= ?" + " AND S_W_ID = ?" + " AND S_I_ID = OL_I_ID"
            + " AND S_QUANTITY < ?";

    private String stockGetCountStockSQLWithVariables7 = "SELECT S_W_ID, S_I_ID, S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DATA" + " FROM " + TPCCConstants.TABLENAME_ORDERLINE + ", "
            + TPCCConstants.TABLENAME_STOCK + " WHERE OL_W_ID = ?" + " AND OL_D_ID = ?" + " AND OL_O_ID < ?" + " AND OL_O_ID >= ?" + " AND S_W_ID = ?" + " AND S_I_ID = OL_I_ID"
            + " AND S_QUANTITY < ?";

    private String stockGetCountStockSQLWithVariables8 = "SELECT S_W_ID, S_I_ID, S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DATA, S_DIST_01" + " FROM " + TPCCConstants.TABLENAME_ORDERLINE + ", "
            + TPCCConstants.TABLENAME_STOCK + " WHERE OL_W_ID = ?" + " AND OL_D_ID = ?" + " AND OL_O_ID < ?" + " AND OL_O_ID >= ?" + " AND S_W_ID = ?" + " AND S_I_ID = OL_I_ID"
            + " AND S_QUANTITY < ?";

    public JSONArray run(Builder builder, Random gen, int terminalWarehouseID, int numWarehouses, int terminalDistrictLowerID, int terminalDistrictUpperID, RESTWorker w) throws SQLException {

        this.builder = builder;

        // TODO: hack
        // The reason this is fixed is so that the first query has the same
        // query shell and feeds into the latter ones
        // If this differs, then we end up with a lot of query shells being
        // unrelated and no spec exec
        // it is not used, it is generated in query itself
        int threshold = 12;// TPCCUtil.randomNumber(10, 20, gen);

        int districtID = TPCCUtil.randomNumber(terminalDistrictLowerID, terminalDistrictUpperID, gen);

        stockLevelTransaction(terminalWarehouseID, districtID, threshold, w);

        return null;
    }

    private void stockLevelTransaction(int w_id, int d_id, int threshold, RESTWorker w) throws SQLException {
        int o_id = 0;
        // XXX int i_id = 0;
        int stock_count = 0;

        // XXX District dist = new District();
        // XXX OrderLine orln = new OrderLine();
        // XXX Stock stck = new Stock();

        JSONArray jsonArray = queryRESTEndpoint(stockGetDistOrderIdSQLWithVariables, Integer.toString(w_id), Integer.toString(d_id));

        if (jsonArray.length() == 0)
            throw new RuntimeException("D_W_ID=" + w_id + " D_ID=" + d_id + " not found!");

        try {
            o_id = jsonArray.getJSONObject(0).getInt("D_NEXT_O_ID");
            threshold = jsonArray.getJSONObject(0).getInt("THRESHOLD");
        } catch (JSONException e) {
            throw new SQLException("Error in parsing the results", e);
        }

        long start = System.nanoTime();
        while (start + 10000000 > System.nanoTime()) {

        }
        jsonArray = queryRESTEndpoint(stockGetCountStockSQLWithVariables, Integer.toString(w_id), Integer.toString(d_id), Integer.toString(o_id), Integer.toString(o_id - 20), Integer.toString(w_id),
                Integer.toString(threshold));

        if (jsonArray.length() == 0)
            throw new RuntimeException("OL_W_ID=" + w_id + " OL_D_ID=" + d_id + " OL_O_ID=" + o_id + " not found!");

        try {
            stock_count = jsonArray.getJSONObject(0).getInt("STOCK_COUNT");
        } catch (JSONException e) {
            throw new SQLException("Error in parsing the results", e);
        }

        // calls same query 8 time
//        start = System.nanoTime();
//        while (start + 500000 > System.nanoTime()) {
//
//        }
//        jsonArray = queryRESTEndpoint(stockGetCountStockSQLWithVariables2, Integer.toString(w_id), Integer.toString(d_id), Integer.toString(o_id), Integer.toString(o_id - 20), Integer.toString(w_id),
//                Integer.toString(threshold));
//        start = System.nanoTime();
//        while (start + 500000 > System.nanoTime()) {
//
//        }
//        jsonArray = queryRESTEndpoint(stockGetCountStockSQLWithVariables3, Integer.toString(w_id), Integer.toString(d_id), Integer.toString(o_id), Integer.toString(o_id - 20), Integer.toString(w_id),
//                Integer.toString(threshold));
//        start = System.nanoTime();
//        while (start + 500000 > System.nanoTime()) {
//
//        }
//        jsonArray = queryRESTEndpoint(stockGetCountStockSQLWithVariables4, Integer.toString(w_id), Integer.toString(d_id), Integer.toString(o_id), Integer.toString(o_id - 20), Integer.toString(w_id),
//                Integer.toString(threshold));
//        start = System.nanoTime();
//        while (start + 500000 > System.nanoTime()) {
//
//        }
//        jsonArray = queryRESTEndpoint(stockGetCountStockSQLWithVariables5, Integer.toString(w_id), Integer.toString(d_id), Integer.toString(o_id), Integer.toString(o_id - 20), Integer.toString(w_id),
//                Integer.toString(threshold));
//        start = System.nanoTime();
//        while (start + 500000 > System.nanoTime()) {
//
//        }
//        jsonArray = queryRESTEndpoint(stockGetCountStockSQLWithVariables6, Integer.toString(w_id), Integer.toString(d_id), Integer.toString(o_id), Integer.toString(o_id - 20), Integer.toString(w_id),
//                Integer.toString(threshold));
//        start = System.nanoTime();
//        while (start + 500000 > System.nanoTime()) {
//
//        }
//        jsonArray = queryRESTEndpoint(stockGetCountStockSQLWithVariables7, Integer.toString(w_id), Integer.toString(d_id), Integer.toString(o_id), Integer.toString(o_id - 20), Integer.toString(w_id),
//                Integer.toString(threshold));
//        start = System.nanoTime();
//        while (start + 500000 > System.nanoTime()) {
//
//        }
//        jsonArray = queryRESTEndpoint(stockGetCountStockSQLWithVariables8, Integer.toString(w_id), Integer.toString(d_id), Integer.toString(o_id), Integer.toString(o_id - 20), Integer.toString(w_id),
//                Integer.toString(threshold));

        StringBuilder terminalMessage = new StringBuilder();
        terminalMessage.append("\n+-------------------------- STOCK-LEVEL --------------------------+");
        terminalMessage.append("\n Warehouse: ");
        terminalMessage.append(w_id);
        terminalMessage.append("\n District:  ");
        terminalMessage.append(d_id);
        terminalMessage.append("\n\n Stock Level Threshold: ");
        terminalMessage.append(threshold);
        terminalMessage.append("\n Low Stock Count:       ");
        terminalMessage.append(stock_count);
        terminalMessage.append("\n+-----------------------------------------------------------------+\n\n");
        if (LOG.isTraceEnabled())
            LOG.trace(terminalMessage.toString());
    }

    private Builder getClient() {
        if (builder == null) {
            new ClientHandlerException("No REST Client, request could not be issued");
        }
        return builder.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON);
    }

    private JSONArray queryRESTEndpoint(String sqlStringWithVariables, String... replacements) throws SQLException {
        String sqlQuery = sqlStringWithVariables;

        for (int i = 0; i < replacements.length; i++) {
            sqlQuery = StringUtils.replaceOnce(sqlQuery, SQL_VARIABLE, replacements[i]);
        }

        ClientResponse response = getClient().post(ClientResponse.class, sqlQuery);
        if (response.getClientResponseStatus() != com.sun.jersey.api.client.ClientResponse.Status.OK) {
            throw new SQLException("Query " + sqlQuery + " encountered an error ");
        }

        JSONArray jsonArray = response.getEntity(JSONArray.class);
        response.close();

        return jsonArray;

    }

}
