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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Random;

import javax.persistence.criteria.CriteriaBuilder.In;
import javax.ws.rs.core.MediaType;

import org.apache.commons.lang3.StringUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.rest.RESTUtil;
import com.oltpbenchmark.benchmarks.rest.RESTWorker;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource.Builder;

public class RESTDelivery extends RESTProcedure {

    private String delivGetOrderIdSQL = "SELECT NO_O_ID FROM " + TPCCConstants.TABLENAME_NEWORDER + " WHERE NO_D_ID = ?" + " AND NO_W_ID = ? ORDER BY NO_O_ID ASC LIMIT 1";
    private String delivDeleteNewOrderSQL = "DELETE FROM " + TPCCConstants.TABLENAME_NEWORDER + "" + " WHERE NO_O_ID = ? AND NO_D_ID = ?" + " AND NO_W_ID = ?";
    private String delivGetCustIdSQL = "SELECT O_C_ID" + " FROM " + TPCCConstants.TABLENAME_OPENORDER + " WHERE O_ID = ?" + " AND O_D_ID = ?" + " AND O_W_ID = ?";
    private String delivUpdateCarrierIdSQL = "UPDATE " + TPCCConstants.TABLENAME_OPENORDER + " SET O_CARRIER_ID = ?" + " WHERE O_ID = ?" + " AND O_D_ID = ?" + " AND O_W_ID = ?";
    private String delivUpdateDeliveryDateSQL = "UPDATE " + TPCCConstants.TABLENAME_ORDERLINE + " SET OL_DELIVERY_D = ?" + " WHERE OL_O_ID = ?" + " AND OL_D_ID = ?" + " AND OL_W_ID = ?";
    private String delivSumOrderAmountSQL = "SELECT SUM(OL_AMOUNT) AS OL_TOTAL" + " FROM " + TPCCConstants.TABLENAME_ORDERLINE + "" + " WHERE OL_O_ID = ?" + " AND OL_D_ID = ?" + " AND OL_W_ID = ?";
    private String delivUpdateCustBalDelivCntSQL = "UPDATE " + TPCCConstants.TABLENAME_CUSTOMER + " SET C_BALANCE = C_BALANCE + ?" + ", C_DELIVERY_CNT = C_DELIVERY_CNT + 1" + " WHERE C_W_ID = ?"
            + " AND C_D_ID = ?" + " AND C_ID = ?";

    private Builder builder;

    public JSONArray run(Builder builder, Random gen, int terminalWarehouseID, int numWarehouses, int terminalDistrictLowerID, int terminalDistrictUpperID, RESTWorker w) throws SQLException {

        this.builder = builder;

        // TODO @anilpacaci, code I have added, it randomly select warehouse,
        // not the assigned one. So each terminal touches all part of database
        if (w.getWorkloadConfiguration().getDistribution().equals("zipfian")) {
            double skew = w.getWorkloadConfiguration().getSkew();
            int limit = (int) w.getWorkloadConfiguration().getScaleFactor();
            terminalWarehouseID = RESTUtil.zipfianRandom(limit, skew);
        } else {
            terminalWarehouseID = TPCCUtil.randomNumber(1, numWarehouses, gen);
        }

        int orderCarrierID = TPCCUtil.randomNumber(1, 10, gen);

        deliveryTransaction(terminalWarehouseID, orderCarrierID, w);
        return null;
    }

    private int deliveryTransaction(int w_id, int o_carrier_id, RESTWorker w) throws SQLException {

        int d_id, c_id;
        double ol_total;
        int[] orderIDs;

        orderIDs = new int[10];
        for (d_id = 1; d_id <= 10; d_id++) {

            JSONArray jsonArray = RESTUtil.executeSelectQuery(builder, delivGetOrderIdSQL, Integer.toString(d_id), Integer.toString(w_id));

            int no_o_id = 0;
            try {
                no_o_id = jsonArray.getJSONObject(0).getInt("NO_O_ID");
            } catch (JSONException e) {
                // This district has no new orders; this can happen but should
                // be rare
                continue;
            }

            orderIDs[d_id - 1] = no_o_id;

            int result = RESTUtil.executeUpdateQuery(builder, delivDeleteNewOrderSQL, Integer.toString(no_o_id), Integer.toString(d_id), Integer.toString(w_id));

            if (result != 1) {
                // This code used to run in a loop in an attempt to make this
                // work
                // with MySQL's default weird consistency level. We just always
                // run
                // this as SERIALIZABLE instead. I don't *think* that fixing
                // this one
                // error makes this work with MySQL's default consistency.
                // Careful
                // auditing would be required.
                throw new UserAbortException("New order w_id=" + w_id + " d_id=" + d_id + " no_o_id=" + no_o_id + " delete failed (not running with SERIALIZABLE isolation?)");
            }

            jsonArray = RESTUtil.executeSelectQuery(builder, delivGetCustIdSQL, Integer.toString(no_o_id), Integer.toString(d_id), Integer.toString(w_id));

            if (jsonArray.length() == 0)
                throw new RuntimeException("NO_O_ID=" + no_o_id + " D_ID=" + d_id + " W_ID=" + w_id + " not found!");

            try {
                c_id = jsonArray.getJSONObject(0).getInt("O_C_ID");
            } catch (JSONException e) {
                throw new SQLException("Error in parsing the results", e);
            }

            result = RESTUtil.executeUpdateQuery(builder, delivUpdateCarrierIdSQL, Integer.toString(o_carrier_id), Integer.toString(no_o_id), Integer.toString(d_id), Integer.toString(w_id));

            if (result != 1)
                throw new RuntimeException("O_ID=" + no_o_id + " O_D_ID=" + d_id + " O_W_ID=" + w_id + " not found!");

            result = RESTUtil.executeUpdateQuery(builder, delivUpdateDeliveryDateSQL, "'" + (new Timestamp(System.currentTimeMillis())).toString() + "'", Integer.toString(no_o_id),
                    Integer.toString(d_id), Integer.toString(w_id));

            if (result == 0)
                throw new RuntimeException("OL_O_ID=" + no_o_id + " OL_D_ID=" + d_id + " OL_W_ID=" + w_id + " not found!");

            jsonArray = RESTUtil.executeSelectQuery(builder, delivSumOrderAmountSQL, Integer.toString(no_o_id), Integer.toString(d_id), Integer.toString(w_id));

            if (jsonArray.length() == 0)
                throw new RuntimeException("OL_O_ID=" + no_o_id + " OL_D_ID=" + d_id + " OL_W_ID=" + w_id + " not found!");
            try {
                ol_total = jsonArray.getJSONObject(0).getDouble("OL_TOTAL");
            } catch (JSONException e) {
                throw new SQLException("Error in parsing the results", e);
            }

            result = RESTUtil.executeUpdateQuery(builder, delivUpdateCustBalDelivCntSQL, Double.toString(ol_total), Integer.toString(w_id), Integer.toString(d_id), Integer.toString(c_id));

            if (result == 0)
                throw new RuntimeException("C_ID=" + c_id + " C_W_ID=" + w_id + " C_D_ID=" + d_id + " not found!");
        }

        // TODO: This part is not used
        StringBuilder terminalMessage = new StringBuilder();
        terminalMessage.append("\n+---------------------------- DELIVERY ---------------------------+\n");
        terminalMessage.append(" Date: ");
        terminalMessage.append(TPCCUtil.getCurrentTime());
        terminalMessage.append("\n\n Warehouse: ");
        terminalMessage.append(w_id);
        terminalMessage.append("\n Carrier:   ");
        terminalMessage.append(o_carrier_id);
        terminalMessage.append("\n\n Delivered Orders\n");
        int skippedDeliveries = 0;
        for (int i = 1; i <= 10; i++) {
            if (orderIDs[i - 1] >= 0) {
                terminalMessage.append("  District ");
                terminalMessage.append(i < 10 ? " " : "");
                terminalMessage.append(i);
                terminalMessage.append(": Order number ");
                terminalMessage.append(orderIDs[i - 1]);
                terminalMessage.append(" was delivered.\n");
            } else {
                terminalMessage.append("  District ");
                terminalMessage.append(i < 10 ? " " : "");
                terminalMessage.append(i);
                terminalMessage.append(": No orders to be delivered.\n");
                skippedDeliveries++;
            }
        }
        terminalMessage.append("+-----------------------------------------------------------------+\n\n");

        return skippedDeliveries;
    }

}