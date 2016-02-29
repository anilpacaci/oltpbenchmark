package com.oltpbenchmark.benchmarks.rest;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Calendar;

import javax.ws.rs.core.MediaType;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.oltpbenchmark.benchmarks.tpcc.pojo.Customer;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource.Builder;

public class RESTUtil {

    private static final String SQL_VARIABLE = "?";

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
        c.c_since = new Timestamp(jsonObject.optLong("C_SINCE"));
        return c;
    }

    public static int zipfianRandom(int limit, double skew) {
        ZipfDistribution zipf = new ZipfDistribution(limit, skew);
        return zipf.sample();
    }

    public static void waitNanoSeconds(long duration) {
        long start = System.nanoTime();
        while (start + duration > System.nanoTime()) {

        }
    }

    public static void waitMilliSeconds(long duration) {
        long start = System.currentTimeMillis();
        while (start + duration > System.currentTimeMillis()) {

        }
    }

    public static Integer executeUpdateQuery(Builder builder, String sqlStringWithVariables, String... replacements) throws SQLException {
        String sqlQuery = sqlStringWithVariables;

        for (int i = 0; i < replacements.length; i++) {
            sqlQuery = StringUtils.replaceOnce(sqlQuery, SQL_VARIABLE, replacements[i]);
        }

        ClientResponse response = RESTUtil.getClient(builder).post(ClientResponse.class, sqlQuery);
        if (response.getClientResponseStatus() != com.sun.jersey.api.client.ClientResponse.Status.OK) {
            throw new SQLException("Query " + sqlQuery + " encountered an error ");
        }

        Integer result = Integer.valueOf(response.getEntity(String.class));
        response.close();

        return result;

    }

    public static JSONArray executeSelectQuery(Builder builder, String sqlStringWithVariables, String... replacements) throws SQLException {
        String sqlQuery = sqlStringWithVariables;

        for (int i = 0; i < replacements.length; i++) {
            sqlQuery = StringUtils.replaceOnce(sqlQuery, SQL_VARIABLE, replacements[i]);
        }

        ClientResponse response = RESTUtil.getClient(builder).post(ClientResponse.class, sqlQuery);
        if (response.getClientResponseStatus() != com.sun.jersey.api.client.ClientResponse.Status.OK) {
            throw new SQLException("Query " + sqlQuery + " encountered an error ");
        }

        JSONArray jsonArray = response.getEntity(JSONArray.class);
        response.close();

        return jsonArray;

    }

    public static Builder getClient(Builder builder) {
        if (builder == null) {
            new ClientHandlerException("No REST Client, request could not be issued");
        }
        return builder.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON);
    }

}
