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
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Random;

import javax.ws.rs.core.MediaType;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.oltpbenchmark.benchmarks.rest.RESTUtil;
import com.oltpbenchmark.benchmarks.rest.RESTWorker;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.pojo.Customer;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource.Builder;

public class RESTOrderStatus extends RESTProcedure {

    private static final Logger LOG = Logger.getLogger(RESTOrderStatus.class);

    private Builder builder;

    private static String SQL_VARIABLE = "?";

    public final String ordStatGetNewestOrdSQLWithVariables = "SELECT O_ID, O_CARRIER_ID, O_ENTRY_D, O_W_ID, O_D_ID  FROM " + TPCCConstants.TABLENAME_OPENORDER + " WHERE O_W_ID = ?"
            + " AND O_D_ID = ? AND O_C_ID = ? ORDER BY O_ID DESC LIMIT 1";

    public final String ordStatGetOrderLinesSQLWithVariables = "SELECT OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY," + " OL_AMOUNT, OL_DELIVERY_D" + " FROM " + TPCCConstants.TABLENAME_ORDERLINE
            + " WHERE OL_O_ID = ?" + " AND OL_D_ID =?" + " AND OL_W_ID = ?";

    public final String ordStatGetOrderLinesSQLWithVariables2 = "SELECT OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY," + " OL_AMOUNT, OL_DELIVERY_D" + " FROM " + TPCCConstants.TABLENAME_ORDERLINE
            + " WHERE OL_O_ID = ?" + " AND OL_D_ID =?" + " AND OL_W_ID = ?";

    public final String ordStatGetOrderLinesSQLWithVariables3 = "SELECT OL_I_ID, OL_QUANTITY," + " OL_AMOUNT, OL_DELIVERY_D" + " FROM " + TPCCConstants.TABLENAME_ORDERLINE + " WHERE OL_O_ID = ?"
            + " AND OL_D_ID =?" + " AND OL_W_ID = ?";

    public final String ordStatGetOrderLinesSQLWithVariables4 = "SELECT OL_QUANTITY," + " OL_AMOUNT, OL_DELIVERY_D" + " FROM " + TPCCConstants.TABLENAME_ORDERLINE + " WHERE OL_O_ID = ?"
            + " AND OL_D_ID =?" + " AND OL_W_ID = ?";

    public final String ordStatGetOrderLinesSQLWithVariables5 = "SELECT" + " OL_AMOUNT, OL_DELIVERY_D" + " FROM " + TPCCConstants.TABLENAME_ORDERLINE + " WHERE OL_O_ID = ?" + " AND OL_D_ID =?"
            + " AND OL_W_ID = ?";

    public final String payGetCustSQLWithVariables = "SELECT C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, " + "C_CITY, C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, "
            + "C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE, C_W_ID, C_D_ID, C_ID FROM " + TPCCConstants.TABLENAME_CUSTOMER + " WHERE " + "C_W_ID = ? AND C_D_ID = ? AND C_ID = ?";

    public final String customerByNameSQLWithVariables = "SELECT C_FIRST, C_MIDDLE, C_ID, C_STREET_1, C_STREET_2, C_CITY, " + "C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, "
            + "C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE, C_W_ID, C_D_ID, C_ID FROM " + TPCCConstants.TABLENAME_CUSTOMER + " WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = '?' ORDER BY C_FIRST";

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

        int districtID = TPCCUtil.randomNumber(terminalDistrictLowerID, terminalDistrictUpperID, gen);
        boolean isCustomerByName = false;
        int y = TPCCUtil.randomNumber(1, 100, gen);
        String customerLastName = null;
        int customerID = -1;
        if (y <= 60) {
            isCustomerByName = true;
            customerLastName = TPCCUtil.getNonUniformRandomLastNameForRun(gen);
            customerID = TPCCUtil.getCustomerID(gen);
        } else {
            isCustomerByName = false;
            customerID = TPCCUtil.getCustomerID(gen);
        }

        try {
            orderStatusTransaction(terminalWarehouseID, districtID, customerID, customerLastName, isCustomerByName, w);
        } catch (JSONException e) {
            throw new SQLException("OrderStatus transaction could NOT be executed", e);
        }
        return null;
    }

    // attention duplicated code across trans... ok for now to maintain separate
    // prepared statements
    public Customer getCustomerById(int c_w_id, int c_d_id, int c_id) throws SQLException, JSONException {

        JSONArray jsonArray = queryRESTEndpoint(payGetCustSQLWithVariables, Integer.toString(c_w_id), Integer.toString(c_d_id), Integer.toString(c_id));

        if (jsonArray.length() == 0) {
            throw new RuntimeException("C_ID=" + c_id + " C_D_ID=" + c_d_id + " C_W_ID=" + c_w_id + " not found!");
        }

        JSONObject jsonObject = jsonArray.getJSONObject(0);
        Customer c = RESTUtil.newCustomerFromResults(jsonObject);
        c.c_id = c_id;
        c.c_last = jsonObject.getString("C_LAST");
        return c;
    }

    private void orderStatusTransaction(int w_id, int d_id, int c_id, String c_last, boolean c_by_name, RESTWorker w) throws SQLException, JSONException {
        int o_id = -1, o_carrier_id = -1;
        Timestamp entdate;
        ArrayList<String> orderLines = new ArrayList<String>();

        Customer c;
        if (c_by_name) {
            assert c_id <= 0;
            // TODO: This only needs c_balance, c_first, c_middle, c_id
            // only fetch those columns?
            c = getCustomerByName(w_id, d_id, c_last);
        } else {
            assert c_last == null;
            c = getCustomerById(w_id, d_id, c_id);
        }

        RESTUtil.waitMilliSeconds(w.getWorkloadConfiguration().getThinktime());

        // find the newest order for the customer
        // retrieve the carrier & order date for the most recent order.

        JSONArray jsonArray = queryRESTEndpoint(ordStatGetNewestOrdSQLWithVariables, Integer.toString(w_id), Integer.toString(d_id), Integer.toString(c.c_id));

        if (jsonArray.length() == 0) {
            throw new RuntimeException("No orders for O_W_ID=" + w_id + " O_D_ID=" + d_id + " O_C_ID=" + c.c_id);
        }

        JSONObject jsonObject = jsonArray.getJSONObject(0);

        o_id = jsonObject.optInt("O_ID");
        o_carrier_id = jsonObject.optInt("O_CARRIER_ID");
        entdate = new Timestamp(jsonObject.optLong("O_ENTRY_D"));

        // retrieve the order lines for the most recent order

        RESTUtil.waitMilliSeconds(w.getWorkloadConfiguration().getThinktime());

        jsonArray = queryRESTEndpoint(ordStatGetOrderLinesSQLWithVariables, Integer.toString(o_id), Integer.toString(d_id), Integer.toString(w_id));

        for (int i = 0; i < jsonArray.length(); i++) {
            jsonObject = jsonArray.getJSONObject(i);
            StringBuilder orderLine = new StringBuilder();
            orderLine.append("[");
            orderLine.append(jsonObject.optLong("OL_SUPPLY_W_ID"));
            orderLine.append(" - ");
            orderLine.append(jsonObject.optLong("OL_I_ID"));
            orderLine.append(" - ");
            orderLine.append(jsonObject.optLong("OL_QUANTITY"));
            orderLine.append(" - ");
            orderLine.append(TPCCUtil.formattedDouble(jsonObject.getDouble("OL_AMOUNT")));
            orderLine.append(" - ");
            if (jsonObject.has("OL_DELIVERY_D"))
                orderLine.append(new Timestamp(jsonObject.optLong("OL_DELIVERY_D")));
            else
                orderLine.append("99-99-9999");
            orderLine.append("]");
            orderLines.add(orderLine.toString());
        }

        StringBuilder terminalMessage = new StringBuilder();
        terminalMessage.append("\n");
        terminalMessage.append("+-------------------------- ORDER-STATUS -------------------------+\n");
        terminalMessage.append(" Date: ");
        terminalMessage.append(TPCCUtil.getCurrentTime());
        terminalMessage.append("\n\n Warehouse: ");
        terminalMessage.append(w_id);
        terminalMessage.append("\n District:  ");
        terminalMessage.append(d_id);
        terminalMessage.append("\n\n Customer:  ");
        terminalMessage.append(c.c_id);
        terminalMessage.append("\n   Name:    ");
        terminalMessage.append(c.c_first);
        terminalMessage.append(" ");
        terminalMessage.append(c.c_middle);
        terminalMessage.append(" ");
        terminalMessage.append(c.c_last);
        terminalMessage.append("\n   Balance: ");
        terminalMessage.append(c.c_balance);
        terminalMessage.append("\n\n");
        if (o_id == -1) {
            terminalMessage.append(" Customer has no orders placed.\n");
        } else {
            terminalMessage.append(" Order-Number: ");
            terminalMessage.append(o_id);
            terminalMessage.append("\n    Entry-Date: ");
            terminalMessage.append(entdate);
            terminalMessage.append("\n    Carrier-Number: ");
            terminalMessage.append(o_carrier_id);
            terminalMessage.append("\n\n");
            if (orderLines.size() != 0) {
                terminalMessage.append(" [Supply_W - Item_ID - Qty - Amount - Delivery-Date]\n");
                for (String orderLine : orderLines) {
                    terminalMessage.append(" ");
                    terminalMessage.append(orderLine);
                    terminalMessage.append("\n");
                }
            } else {
                if (LOG.isTraceEnabled())
                    LOG.trace(" This Order has no Order-Lines.\n");
            }
        }
        terminalMessage.append("+-----------------------------------------------------------------+\n\n");
        if (LOG.isTraceEnabled())
            LOG.trace(terminalMessage.toString());
    }

    // attention this code is repeated in other transacitons... ok for now to
    // allow for separate statements.
    public Customer getCustomerByName(int c_w_id, int c_d_id, String c_last) throws SQLException, JSONException {
        ArrayList<Customer> customers = new ArrayList<Customer>();

        String customerByNameSQL = customerByNameSQLWithVariables;

        customerByNameSQL = StringUtils.replaceOnce(customerByNameSQL, SQL_VARIABLE, Integer.toString(c_w_id));
        customerByNameSQL = StringUtils.replaceOnce(customerByNameSQL, SQL_VARIABLE, Integer.toString(c_d_id));
        customerByNameSQL = StringUtils.replaceOnce(customerByNameSQL, SQL_VARIABLE, c_last);

        ClientResponse response = getClient().post(ClientResponse.class, customerByNameSQL);
        if (response.getClientResponseStatus() != com.sun.jersey.api.client.ClientResponse.Status.OK) {
            throw new SQLException("Query " + customerByNameSQL + " encountered an error ");
        }

        JSONArray jsonArray = response.getEntity(JSONArray.class);
        response.close();

        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject jsonObject = jsonArray.getJSONObject(i);
            Customer c = RESTUtil.newCustomerFromResults(jsonObject);
            c.c_id = jsonObject.getInt("C_ID");
            c.c_last = c_last;
            customers.add(c);
        }

        if (customers.size() == 0) {
            throw new RuntimeException("C_LAST=" + c_last + " C_D_ID=" + c_d_id + " C_W_ID=" + c_w_id + " not found!");
        }

        // TPC-C 2.5.2.2: Position n / 2 rounded up to the next integer, but
        // that
        // counts starting from 1.
        int index = customers.size() / 2;
        if (customers.size() % 2 == 0) {
            index -= 1;
        }
        return customers.get(index);
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

    private void waitMilliSeconds(long duration) {
        long start = System.currentTimeMillis();
        while (start + duration > System.currentTimeMillis()) {

        }
    }

    private void waitNanoSeconds(long duration) {
        long start = System.nanoTime();
        while (start + duration > System.nanoTime()) {

        }
    }
}
