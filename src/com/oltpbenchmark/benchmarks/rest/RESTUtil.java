package com.oltpbenchmark.benchmarks.rest;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.oltpbenchmark.benchmarks.tpcc.pojo.Customer;

public class RESTUtil {

    /**
     * Creates a Customer object from the current row in the given JSONObject.
     * The caller is responsible for closing the ResultSet.
     * 
     * @param jsonObject
     *            JSONObject positioned to the desired row
     * @return the newly created Customer object
     * @throws SQLException
     *             for problems getting data from row
     * @throws JSONException
     */
    public static Customer newCustomerFromResults(JSONObject jsonObject) throws JSONException {
        Customer c = new Customer();
        // TODO: Use column indices: probably faster?
        c.c_first = jsonObject.optString("C_FIRST");
        c.c_middle = jsonObject.optString("C_MIDDLE");
        c.c_street_1 = jsonObject.optString("C_STREET_1");
        c.c_street_2 = jsonObject.optString("C_STREET_2");
        c.c_city = jsonObject.optString("C_STATE");
        c.c_zip = jsonObject.optString("C_ZIP");
        c.c_phone = jsonObject.optString("C_PHONE");
        c.c_credit = jsonObject.optString("C_CREDIT");
        c.c_credit_lim = (float) jsonObject.optDouble("C_CREDIT_LIM");
        c.c_discount = (float) jsonObject.optDouble("C_DISCOUNT");
        c.c_balance = (float) jsonObject.optDouble("C_BALANCE");
        c.c_ytd_payment = (float) jsonObject.optDouble("C_YTD_PAYMENT");
        c.c_payment_cnt = jsonObject.optInt("C_PAYMENT_CNT");
        c.c_since = Timestamp.from(Instant.ofEpochMilli(jsonObject.optLong("C_SINCE")));
        return c;
    }

}
