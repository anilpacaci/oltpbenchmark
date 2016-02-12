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
import java.util.ArrayList;
import java.util.Random;

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.oltpbenchmark.benchmarks.rest.RESTUtil;
import com.oltpbenchmark.benchmarks.rest.RESTWorker;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig;
import com.oltpbenchmark.benchmarks.tpcc.pojo.Customer;
import com.sun.jersey.api.client.WebResource.Builder;

public class RESTPayment extends RESTProcedure {

    private static final Logger LOG = Logger.getLogger(RESTPayment.class);

    private String payUpdateWhseSQL = ("UPDATE " + TPCCConstants.TABLENAME_WAREHOUSE + " SET W_YTD = W_YTD + ?  WHERE W_ID = ? ");
    private String payGetWhseSQL = ("SELECT W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_NAME" + " FROM " + TPCCConstants.TABLENAME_WAREHOUSE + " WHERE W_ID = ?");
    private String payUpdateDistSQL = ("UPDATE " + TPCCConstants.TABLENAME_DISTRICT + " SET D_YTD = D_YTD + ? WHERE D_W_ID = ? AND D_ID = ?");
    private String payGetDistSQL = ("SELECT D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_NAME" + " FROM " + TPCCConstants.TABLENAME_DISTRICT + " WHERE D_W_ID = ? AND D_ID = ?");
    private String payGetCustSQL = ("SELECT C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, " + "C_CITY, C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, "
            + "C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE FROM " + TPCCConstants.TABLENAME_CUSTOMER + " WHERE " + "C_W_ID = ? AND C_D_ID = ? AND C_ID = ?");
    private String payGetCustCdataSQL = ("SELECT C_DATA FROM " + TPCCConstants.TABLENAME_CUSTOMER + " WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?");
    private String payUpdateCustBalCdataSQL = ("UPDATE " + TPCCConstants.TABLENAME_CUSTOMER + " SET C_BALANCE = ?, C_YTD_PAYMENT = ?, " + "C_PAYMENT_CNT = ?, C_DATA = ? "
            + "WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?");
    private String payUpdateCustBalSQL = ("UPDATE " + TPCCConstants.TABLENAME_CUSTOMER + " SET C_BALANCE = ?, C_YTD_PAYMENT = ?, " + "C_PAYMENT_CNT = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?");
    private String payInsertHistSQL = ("INSERT INTO " + TPCCConstants.TABLENAME_HISTORY + " (H_C_D_ID, H_C_W_ID, H_C_ID, H_D_ID, H_W_ID, H_DATE, H_AMOUNT, H_DATA) " + " VALUES (?,?,?,?,?,?,?,?)");
    private String customerByNameSQL = ("SELECT C_FIRST, C_MIDDLE, C_ID, C_STREET_1, C_STREET_2, C_CITY, " + "C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, "
            + "C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE FROM " + TPCCConstants.TABLENAME_CUSTOMER + " " + "WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST");

    private Builder builder;

    public JSONArray run(Builder builder, Random gen, int terminalWarehouseID, int numWarehouses, int terminalDistrictLowerID, int terminalDistrictUpperID, RESTWorker w) throws SQLException {

        // payUpdateWhse =this.getPreparedStatement(conn, payUpdateWhseSQL);

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
        int customerID = TPCCUtil.getCustomerID(gen);

        int x = TPCCUtil.randomNumber(1, 100, gen);
        int customerDistrictID;
        int customerWarehouseID;
        if (x <= 85) {
            customerDistrictID = districtID;
            customerWarehouseID = terminalWarehouseID;
        } else {
            customerDistrictID = TPCCUtil.randomNumber(1, jTPCCConfig.configDistPerWhse, gen);
            do {
                customerWarehouseID = TPCCUtil.randomNumber(1, numWarehouses, gen);
            } while (customerWarehouseID == terminalWarehouseID && numWarehouses > 1);
        }

        long y = TPCCUtil.randomNumber(1, 100, gen);
        boolean customerByName;
        String customerLastName = null;
        customerID = -1;
        if (y <= 60) {
            // 60% lookups by last name
            customerByName = true;
            customerLastName = TPCCUtil.getNonUniformRandomLastNameForRun(gen);
        } else {
            // 40% lookups by customer ID
            customerByName = false;
            customerID = TPCCUtil.getCustomerID(gen);
        }

        float paymentAmount = (float) (TPCCUtil.randomNumber(100, 500000, gen) / 100.0);

        try {
            paymentTransaction(terminalWarehouseID, customerWarehouseID, paymentAmount, districtID, customerDistrictID, customerID, customerLastName, customerByName, w);
        } catch (JSONException e) {
            throw new SQLException("Payment Transaction could NOT be executed", e);
        }

        return null;
    }

    private void paymentTransaction(int w_id, int c_w_id, float h_amount, int d_id, int c_d_id, int c_id, String c_last, boolean c_by_name, RESTWorker w) throws SQLException, JSONException {
        String w_street_1, w_street_2, w_city, w_state, w_zip, w_name;
        String d_street_1, d_street_2, d_city, d_state, d_zip, d_name;

        int result = RESTUtil.executeUpdateQuery(builder, payUpdateWhseSQL, Float.toString(h_amount), Integer.toString(w_id));

        if (result == 0)
            throw new RuntimeException("W_ID=" + w_id + " not found!");

        JSONArray resultSet = RESTUtil.executeSelectQuery(builder, payGetWhseSQL, Integer.toString(w_id));

        if (resultSet.length() == 0)
            throw new RuntimeException("W_ID=" + w_id + " not found!");

        JSONObject rs = resultSet.getJSONObject(0);
        w_street_1 = rs.getString("W_STREET_1");
        w_street_2 = rs.getString("W_STREET_2");
        w_city = rs.getString("W_CITY");
        w_state = rs.getString("W_STATE");
        w_zip = rs.getString("W_ZIP");
        w_name = rs.getString("W_NAME");

        result = RESTUtil.executeUpdateQuery(builder, payUpdateDistSQL, Float.toString(h_amount), Integer.toString(w_id), Integer.toString(d_id));

        if (result == 0)
            throw new RuntimeException("D_ID=" + d_id + " D_W_ID=" + w_id + " not found!");

        resultSet = RESTUtil.executeSelectQuery(builder, payGetDistSQL, Integer.toString(w_id), Integer.toString(d_id));

        if (resultSet.length() == 0)
            throw new RuntimeException("D_ID=" + d_id + " D_W_ID=" + w_id + " not found!");

        rs = resultSet.getJSONObject(0);
        d_street_1 = rs.getString("D_STREET_1");
        d_street_2 = rs.getString("D_STREET_2");
        d_city = rs.getString("D_CITY");
        d_state = rs.getString("D_STATE");
        d_zip = rs.getString("D_ZIP");
        d_name = rs.getString("D_NAME");

        Customer c;
        if (c_by_name) {
            assert c_id <= 0;
            c = getCustomerByName(c_w_id, c_d_id, c_last);
        } else {
            assert c_last == null;
            c = getCustomerById(c_w_id, c_d_id, c_id);
        }

        c.c_balance -= h_amount;
        c.c_ytd_payment += h_amount;
        c.c_payment_cnt += 1;
        String c_data = null;
        if (c.c_credit.equals("BC")) { // bad credit

            resultSet = RESTUtil.executeSelectQuery(builder, payGetCustCdataSQL, Integer.toString(c_w_id), Integer.toString(c_d_id), Integer.toString(c.c_d_id));

            if (resultSet.length() == 0)
                throw new RuntimeException("C_ID=" + c.c_id + " C_W_ID=" + c_w_id + " C_D_ID=" + c_d_id + " not found!");

            rs = resultSet.getJSONObject(0);
            c_data = rs.getString("C_DATA");

            c_data = c.c_id + " " + c_d_id + " " + c_w_id + " " + d_id + " " + w_id + " " + h_amount + " | " + c_data;
            if (c_data.length() > 500)
                c_data = c_data.substring(0, 500);

            result = RESTUtil.executeUpdateQuery(builder, payUpdateCustBalCdataSQL, Float.toString(c.c_balance), Float.toString(c.c_ytd_payment), Float.toString(c.c_payment_cnt), c_data,
                    Integer.toString(c_w_id), Integer.toString(c_d_id), Integer.toString(c.c_id));

            if (result == 0)
                throw new RuntimeException("Error in PYMNT Txn updating Customer C_ID=" + c.c_id + " C_W_ID=" + c_w_id + " C_D_ID=" + c_d_id);

        } else { // GoodCredit

            result = RESTUtil.executeUpdateQuery(builder, payUpdateCustBalSQL, Float.toString(c.c_balance), Float.toString(c.c_ytd_payment), Float.toString(c.c_payment_cnt),
                    Integer.toString(c_w_id), Integer.toString(c_d_id), Integer.toString(c.c_id));

            if (result == 0)
                throw new RuntimeException("C_ID=" + c.c_id + " C_W_ID=" + c_w_id + " C_D_ID=" + c_d_id + " not found!");

        }

        if (w_name.length() > 10)
            w_name = w_name.substring(0, 10);
        if (d_name.length() > 10)
            d_name = d_name.substring(0, 10);
        String h_data = w_name + "    " + d_name;

        result = RESTUtil.executeUpdateQuery(builder, payInsertHistSQL, Integer.toString(c_d_id), Integer.toString(c_w_id), Integer.toString(c.c_id), Integer.toString(d_id),
                Integer.toString(w_id), "'" + (new Timestamp(System.currentTimeMillis())).toString() + "'", Float.toString(h_amount), "'" + h_data + "'");

        StringBuilder terminalMessage = new StringBuilder();
        terminalMessage.append("\n+---------------------------- PAYMENT ----------------------------+");
        terminalMessage.append("\n Date: " + TPCCUtil.getCurrentTime());
        terminalMessage.append("\n\n Warehouse: ");
        terminalMessage.append(w_id);
        terminalMessage.append("\n   Street:  ");
        terminalMessage.append(w_street_1);
        terminalMessage.append("\n   Street:  ");
        terminalMessage.append(w_street_2);
        terminalMessage.append("\n   City:    ");
        terminalMessage.append(w_city);
        terminalMessage.append("   State: ");
        terminalMessage.append(w_state);
        terminalMessage.append("  Zip: ");
        terminalMessage.append(w_zip);
        terminalMessage.append("\n\n District:  ");
        terminalMessage.append(d_id);
        terminalMessage.append("\n   Street:  ");
        terminalMessage.append(d_street_1);
        terminalMessage.append("\n   Street:  ");
        terminalMessage.append(d_street_2);
        terminalMessage.append("\n   City:    ");
        terminalMessage.append(d_city);
        terminalMessage.append("   State: ");
        terminalMessage.append(d_state);
        terminalMessage.append("  Zip: ");
        terminalMessage.append(d_zip);
        terminalMessage.append("\n\n Customer:  ");
        terminalMessage.append(c.c_id);
        terminalMessage.append("\n   Name:    ");
        terminalMessage.append(c.c_first);
        terminalMessage.append(" ");
        terminalMessage.append(c.c_middle);
        terminalMessage.append(" ");
        terminalMessage.append(c.c_last);
        terminalMessage.append("\n   Street:  ");
        terminalMessage.append(c.c_street_1);
        terminalMessage.append("\n   Street:  ");
        terminalMessage.append(c.c_street_2);
        terminalMessage.append("\n   City:    ");
        terminalMessage.append(c.c_city);
        terminalMessage.append("   State: ");
        terminalMessage.append(c.c_state);
        terminalMessage.append("  Zip: ");
        terminalMessage.append(c.c_zip);
        terminalMessage.append("\n   Since:   ");
        if (c.c_since != null) {
            terminalMessage.append(c.c_since.toString());
        } else {
            terminalMessage.append("");
        }
        terminalMessage.append("\n   Credit:  ");
        terminalMessage.append(c.c_credit);
        terminalMessage.append("\n   %Disc:   ");
        terminalMessage.append(c.c_discount);
        terminalMessage.append("\n   Phone:   ");
        terminalMessage.append(c.c_phone);
        terminalMessage.append("\n\n Amount Paid:      ");
        terminalMessage.append(h_amount);
        terminalMessage.append("\n Credit Limit:     ");
        terminalMessage.append(c.c_credit_lim);
        terminalMessage.append("\n New Cust-Balance: ");
        terminalMessage.append(c.c_balance);
        if (c.c_credit.equals("BC")) {
            if (c_data.length() > 50) {
                terminalMessage.append("\n\n Cust-Data: " + c_data.substring(0, 50));
                int data_chunks = c_data.length() > 200 ? 4 : c_data.length() / 50;
                for (int n = 1; n < data_chunks; n++)
                    terminalMessage.append("\n            " + c_data.substring(n * 50, (n + 1) * 50));
            } else {
                terminalMessage.append("\n\n Cust-Data: " + c_data);
            }
        }
        terminalMessage.append("\n+-----------------------------------------------------------------+\n\n");

        if (LOG.isTraceEnabled())
            LOG.trace(terminalMessage.toString());

    }

    // attention duplicated code across trans... ok for now to maintain separate
    // prepared statements
    public Customer getCustomerById(int c_w_id, int c_d_id, int c_id) throws SQLException, JSONException {

        JSONArray result = RESTUtil.executeSelectQuery(builder, payGetCustSQL, Integer.toString(c_w_id), Integer.toString(c_d_id), Integer.toString(c_id));

        if (result.length() == 0) {
            throw new RuntimeException("C_ID=" + c_id + " C_D_ID=" + c_d_id + " C_W_ID=" + c_w_id + " not found!");
        }

        JSONObject jsonObject = result.getJSONObject(0);
        Customer c = RESTUtil.newCustomerFromResults(jsonObject);
        c.c_id = c_id;
        c.c_last = jsonObject.getString("C_LAST");
        return c;
    }

    // attention this code is repeated in other transacitons... ok for now to
    // allow for separate statements.
    public Customer getCustomerByName(int c_w_id, int c_d_id, String c_last) throws SQLException, JSONException {
        ArrayList<Customer> customers = new ArrayList<Customer>();

        JSONArray result = RESTUtil.executeSelectQuery(builder, customerByNameSQL, Integer.toString(c_w_id), Integer.toString(c_d_id), "'" + c_last + "'");

        for (int i = 0; i < result.length(); i++) {
            JSONObject jsonObject = result.getJSONObject(i);
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

}
