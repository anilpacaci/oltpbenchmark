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

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.rest.RESTUtil;
import com.oltpbenchmark.benchmarks.rest.RESTWorker;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;
import com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig;
import com.sun.jersey.api.client.WebResource.Builder;

public class RESTNewOrder extends RESTProcedure {

    private static final Logger LOG = Logger.getLogger(RESTNewOrder.class);

    private String stmtGetCustWhseSQL = ("SELECT C_DISCOUNT, C_LAST, C_CREDIT, W_TAX" + "  FROM " + TPCCConstants.TABLENAME_CUSTOMER + ", " + TPCCConstants.TABLENAME_WAREHOUSE
            + " WHERE W_ID = ? AND C_W_ID = ?" + " AND C_D_ID = ? AND C_ID = ?");

    private String stmtGetDistSQL = ("SELECT D_NEXT_O_ID, D_TAX FROM " + TPCCConstants.TABLENAME_DISTRICT + " WHERE D_W_ID = ? AND D_ID = ? "
    // FOR UPDATE"
    );

    private String stmtInsertNewOrderSQL = ("INSERT INTO " + TPCCConstants.TABLENAME_NEWORDER + " (NO_O_ID, NO_D_ID, NO_W_ID) VALUES ( ?, ?, ?)");

    private String stmtUpdateDistSQL = ("UPDATE " + TPCCConstants.TABLENAME_DISTRICT + " SET D_NEXT_O_ID = D_NEXT_O_ID + 1 WHERE D_W_ID = ? AND D_ID = ?");

    private String stmtInsertOOrderSQL = ("INSERT INTO " + TPCCConstants.TABLENAME_OPENORDER + " (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL)" + " VALUES (?, ?, ?, ?, ?, ?, ?)");

    private String stmtGetItemSQL = ("SELECT I_PRICE, I_NAME , I_DATA FROM " + TPCCConstants.TABLENAME_ITEM + " WHERE I_ID = ?");

    private String stmtGetStockSQL = ("SELECT S_QUANTITY, S_DATA, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, " + "       S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10" + " FROM "
            + TPCCConstants.TABLENAME_STOCK + " WHERE S_I_ID = ? AND S_W_ID = ? "); // FOR
                                                                                    // UPDATE");

    private String stmtUpdateStockSQL = ("UPDATE " + TPCCConstants.TABLENAME_STOCK + " SET S_QUANTITY = ? , S_YTD = S_YTD + ?, S_ORDER_CNT = S_ORDER_CNT + 1, S_REMOTE_CNT = S_REMOTE_CNT + ? "
            + " WHERE S_I_ID = ? AND S_W_ID = ?");

    private String stmtInsertOrderLineSQL = ("INSERT INTO " + TPCCConstants.TABLENAME_ORDERLINE + " (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID,"
            + "  OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES (?,?,?,?,?,?,?,?,?)");

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

        int districtID = TPCCUtil.randomNumber(terminalDistrictLowerID, terminalDistrictUpperID, gen);
        int customerID = TPCCUtil.getCustomerID(gen);

        int numItems = (int) TPCCUtil.randomNumber(5, 15, gen);
        int[] itemIDs = new int[numItems];
        int[] supplierWarehouseIDs = new int[numItems];
        int[] orderQuantities = new int[numItems];
        int allLocal = 1;
        for (int i = 0; i < numItems; i++) {
            itemIDs[i] = TPCCUtil.getItemID(gen);
            if (TPCCUtil.randomNumber(1, 100, gen) > 1) {
                supplierWarehouseIDs[i] = terminalWarehouseID;
            } else {
                do {
                    supplierWarehouseIDs[i] = TPCCUtil.randomNumber(1, numWarehouses, gen);
                } while (supplierWarehouseIDs[i] == terminalWarehouseID && numWarehouses > 1);
                allLocal = 0;
            }
            orderQuantities[i] = TPCCUtil.randomNumber(1, 10, gen);
        }

        // we need to cause 1% of the new orders to be rolled back.
        if (TPCCUtil.randomNumber(1, 100, gen) == 1)
            itemIDs[numItems - 1] = jTPCCConfig.INVALID_ITEM_ID;

        try {
            newOrderTransaction(terminalWarehouseID, districtID, customerID, numItems, allLocal, itemIDs, supplierWarehouseIDs, orderQuantities, w);
        } catch (JSONException e) {
            throw new SQLException("NewOrder Transaction could NOT be executed", e);
        }
        return null;

    }

    private void newOrderTransaction(int w_id, int d_id, int c_id, int o_ol_cnt, int o_all_local, int[] itemIDs, int[] supplierWarehouseIDs, int[] orderQuantities, RESTWorker w)
            throws SQLException, JSONException {
        double c_discount, w_tax, d_tax = 0, i_price;
        int d_next_o_id, o_id = -1, s_quantity;
        String c_last = null, c_credit = null, i_name, i_data, s_data;
        String s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05;
        String s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, ol_dist_info = null;
        double[] itemPrices = new double[o_ol_cnt];
        double[] orderLineAmounts = new double[o_ol_cnt];
        String[] itemNames = new String[o_ol_cnt];
        int[] stockQuantities = new int[o_ol_cnt];
        char[] brandGeneric = new char[o_ol_cnt];
        int ol_supply_w_id, ol_i_id, ol_quantity;
        int s_remote_cnt_increment;
        double ol_amount, total_amount = 0;
        try {

            JSONArray resultSet = RESTUtil.executeSelectQuery(builder, stmtGetCustWhseSQL, Integer.toString(w_id), Integer.toString(w_id), Integer.toString(d_id), Integer.toString(c_id));
            if (resultSet.length() == 0)
                throw new RuntimeException("W_ID=" + w_id + " C_D_ID=" + d_id + " C_ID=" + c_id + " not found!");

            JSONObject rs = resultSet.getJSONObject(0);
            c_discount = rs.getDouble("C_DISCOUNT");
            c_last = rs.getString("C_LAST");
            c_credit = rs.getString("C_CREDIT");
            w_tax = rs.getDouble("W_TAX");

            resultSet = RESTUtil.executeSelectQuery(builder, stmtGetDistSQL, Integer.toString(w_id), Integer.toString(d_id));

            if (resultSet.length() == 0) {
                throw new RuntimeException("D_ID=" + d_id + " D_W_ID=" + w_id + " not found!");
            }

            rs = resultSet.getJSONObject(0);
            d_next_o_id = rs.getInt("D_NEXT_O_ID");
            d_tax = rs.getDouble("D_TAX");

            // woonhak, need to change order because of foreign key constraints
            // update next_order_id first, but it might doesn't matter

            int result = RESTUtil.executeUpdateQuery(builder, stmtUpdateDistSQL, Integer.toString(w_id), Integer.toString(d_id));
            if (result == 0)
                throw new RuntimeException("Error!! Cannot update next_order_id on district for D_ID=" + d_id + " D_W_ID=" + w_id);

            o_id = d_next_o_id;

            // woonhak, need to change order, because of foreign key constraints
            // [[insert ooder first
            result = RESTUtil.executeUpdateQuery(builder, stmtInsertOOrderSQL, Integer.toString(o_id), Integer.toString(d_id), Integer.toString(w_id), Integer.toString(c_id),
                    "'" + (new Timestamp(System.currentTimeMillis())).toString() + "'", Integer.toString(o_ol_cnt), Integer.toString(o_all_local));

            // insert ooder first]]
            if (result == 0)
                throw new RuntimeException("Error!! Cannot update InsertOOrder ");

            result = RESTUtil.executeUpdateQuery(builder, stmtInsertNewOrderSQL, Integer.toString(o_id), Integer.toString(d_id), Integer.toString(w_id));
            if (result == 0)
                throw new RuntimeException("Error!! Cannot update InsertNewOrder ");

            /*
             * woonhak, [[change order stmtInsertOOrder.setInt(1, o_id);
             * stmtInsertOOrder.setInt(2, d_id); stmtInsertOOrder.setInt(3,
             * w_id); stmtInsertOOrder.setInt(4, c_id);
             * stmtInsertOOrder.setTimestamp(5, new
             * Timestamp(System.currentTimeMillis()));
             * stmtInsertOOrder.setInt(6, o_ol_cnt); stmtInsertOOrder.setInt(7,
             * o_all_local); stmtInsertOOrder.executeUpdate(); change order]]
             */

            for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
                ol_supply_w_id = supplierWarehouseIDs[ol_number - 1];
                ol_i_id = itemIDs[ol_number - 1];
                ol_quantity = orderQuantities[ol_number - 1];

                resultSet = RESTUtil.executeSelectQuery(builder, stmtGetItemSQL, Integer.toString(ol_i_id));
                if (resultSet.length() == 0) {
                    // This is (hopefully) an expected error: this is an
                    // expected new order rollback
                    assert ol_number == o_ol_cnt;
                    assert ol_i_id == jTPCCConfig.INVALID_ITEM_ID;
                    throw new UserAbortException("EXPECTED new order rollback: I_ID=" + ol_i_id + " not found!");
                }

                rs = resultSet.getJSONObject(0);
                i_price = rs.getDouble("I_PRICE");
                i_name = rs.getString("I_NAME");
                i_data = rs.getString("I_DATA");

                itemPrices[ol_number - 1] = i_price;
                itemNames[ol_number - 1] = i_name;

                resultSet = RESTUtil.executeSelectQuery(builder, stmtGetStockSQL, Integer.toString(ol_i_id), Integer.toString(ol_supply_w_id));
                if (resultSet.length() == 0)
                    throw new RuntimeException("I_ID=" + ol_i_id + " not found!");
                rs = resultSet.getJSONObject(0);
                s_quantity = rs.getInt("S_QUANTITY");
                s_data = rs.getString("S_DATA");
                s_dist_01 = rs.getString("S_DIST_01");
                s_dist_02 = rs.getString("S_DIST_02");
                s_dist_03 = rs.getString("S_DIST_03");
                s_dist_04 = rs.getString("S_DIST_04");
                s_dist_05 = rs.getString("S_DIST_05");
                s_dist_06 = rs.getString("S_DIST_06");
                s_dist_07 = rs.getString("S_DIST_07");
                s_dist_08 = rs.getString("S_DIST_08");
                s_dist_09 = rs.getString("S_DIST_09");
                s_dist_10 = rs.getString("S_DIST_10");

                stockQuantities[ol_number - 1] = s_quantity;

                if (s_quantity - ol_quantity >= 10) {
                    s_quantity -= ol_quantity;
                } else {
                    s_quantity += -ol_quantity + 91;
                }

                if (ol_supply_w_id == w_id) {
                    s_remote_cnt_increment = 0;
                } else {
                    s_remote_cnt_increment = 1;
                }

                result = RESTUtil.executeUpdateQuery(builder, stmtUpdateStockSQL, Integer.toString(s_quantity), Integer.toString(ol_quantity), Integer.toString(s_remote_cnt_increment),
                        Integer.toString(ol_i_id), Integer.toString(ol_supply_w_id));

                ol_amount = ol_quantity * i_price;
                orderLineAmounts[ol_number - 1] = ol_amount;
                total_amount += ol_amount;

                if (i_data.indexOf("GENERIC") != -1 && s_data.indexOf("GENERIC") != -1) {
                    brandGeneric[ol_number - 1] = 'B';
                } else {
                    brandGeneric[ol_number - 1] = 'G';
                }

                switch ((int) d_id) {
                    case 1:
                        ol_dist_info = s_dist_01;
                        break;
                    case 2:
                        ol_dist_info = s_dist_02;
                        break;
                    case 3:
                        ol_dist_info = s_dist_03;
                        break;
                    case 4:
                        ol_dist_info = s_dist_04;
                        break;
                    case 5:
                        ol_dist_info = s_dist_05;
                        break;
                    case 6:
                        ol_dist_info = s_dist_06;
                        break;
                    case 7:
                        ol_dist_info = s_dist_07;
                        break;
                    case 8:
                        ol_dist_info = s_dist_08;
                        break;
                    case 9:
                        ol_dist_info = s_dist_09;
                        break;
                    case 10:
                        ol_dist_info = s_dist_10;
                        break;
                }

                result = RESTUtil.executeUpdateQuery(builder, stmtInsertOrderLineSQL, Integer.toString(o_id), Integer.toString(d_id), Integer.toString(w_id), Integer.toString(ol_number),
                        Integer.toString(ol_i_id), Integer.toString(ol_supply_w_id), Integer.toString(ol_quantity), Double.toString(ol_amount), "'" + ol_dist_info + "'");

            } // end-for

            total_amount *= (1 + w_tax + d_tax) * (1 - c_discount);
        } catch (UserAbortException userEx) {
            LOG.debug("Caught an expected error in New Order");
            throw userEx;
        } finally {
        }

    }

}
