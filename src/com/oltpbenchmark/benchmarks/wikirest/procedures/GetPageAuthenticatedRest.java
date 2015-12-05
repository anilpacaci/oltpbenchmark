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

package com.oltpbenchmark.benchmarks.wikirest.procedures;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.ws.rs.core.MediaType;

import org.apache.commons.lang3.StringUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.wikipedia.WikipediaConstants;
import com.oltpbenchmark.benchmarks.wikipedia.util.Article;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource.Builder;

public class GetPageAuthenticatedRest extends Procedure {

    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------

    public final String selectPageWithVariables = "SELECT * FROM " + WikipediaConstants.TABLENAME_PAGE + " WHERE page_namespace = ? AND page_title = '?' LIMIT 1";
    public final String selectPageRestrictionWithVariables = "SELECT * FROM " + WikipediaConstants.TABLENAME_PAGE_RESTRICTIONS + " WHERE pr_page = ?";
    public final String selectIpBlocksWithVariables = "SELECT * FROM " + WikipediaConstants.TABLENAME_IPBLOCKS + " WHERE ipb_user = ?";
    public final String selectPageRevisionWithVariables = "SELECT * " + "  FROM " + WikipediaConstants.TABLENAME_PAGE + ", " + WikipediaConstants.TABLENAME_REVISION + " WHERE page_id = rev_page "
            + "   AND rev_page = ? " + "   AND page_id = ? " + "   AND rev_id = page_latest LIMIT 1";
    public final String selectTextWithVariables = "SELECT old_text,old_flags FROM " + WikipediaConstants.TABLENAME_TEXT + " WHERE old_id = ? LIMIT 1";
    public final String selectUserWithVariables = "SELECT * FROM " + WikipediaConstants.TABLENAME_USER + " WHERE user_id = ? LIMIT 1";
    public final String selectGroupWithVariables = "SELECT ug_group FROM " + WikipediaConstants.TABLENAME_USER_GROUPS + " WHERE ug_user = ?";

    private final static String SQL_VARIABLE = "?";

    private Builder builder;

    // -----------------------------------------------------------------
    // RUN
    // -----------------------------------------------------------------

    public Article run(Builder builder, boolean forSelect, String userIp, int userId, int nameSpace, String pageTitle) throws SQLException, JSONException {
        // =======================================================
        // LOADING BASIC DATA: txn1
        // =======================================================
        // Retrieve the user data, if the user is logged in

        this.builder = builder;

        // FIXME TOO FREQUENTLY SELECTING BY USER_ID
        String userText = userIp;
        JSONArray jsonArray;
        if (userId > 0) {
            jsonArray = queryRESTEndpoint(selectUserWithVariables, Integer.toString(userId));

            if (jsonArray.length() == 0) {
                throw new UserAbortException("Invalid UserId: " + userId);
            }

            userText = jsonArray.getJSONObject(0).getString("user_name");
            // Fetch all groups the user might belong to (access control
            // information)

            jsonArray = queryRESTEndpoint(selectGroupWithVariables, Integer.toString(userId));
        }

        jsonArray = queryRESTEndpoint(selectPageWithVariables, Integer.toString(nameSpace), pageTitle);

        if (jsonArray.length() == 0) {
            throw new UserAbortException("INVALID page namespace/title:" + nameSpace + "/" + pageTitle);
        }

        int pageId = jsonArray.getJSONObject(0).getInt("page_id");

        jsonArray = queryRESTEndpoint(selectPageRestrictionWithVariables, Integer.toString(pageId));

        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject jsonObject = jsonArray.getJSONObject(i);
        }

        jsonArray = queryRESTEndpoint(selectIpBlocksWithVariables, Integer.toString(userId));

        // check using blocking of a user by either the IP address or the
        // user_name
        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject jsonObject = jsonArray.getJSONObject(i);
        }

        jsonArray = queryRESTEndpoint(selectPageRevisionWithVariables, Integer.toString(pageId), Integer.toString(pageId));
        if (jsonArray.length() == 0) {
            throw new UserAbortException("no such revision: page_id:" + pageId + " page_namespace: " + nameSpace + " page_title:" + pageTitle);
        }

        int revisionId = jsonArray.getJSONObject(0).getInt("rev_id");
        int textId = jsonArray.getJSONObject(0).getInt("rev_text_id");

        // NOTE: the following is our variation of wikipedia... the original did
        // not contain old_page column!StmtStmt
        // sql =
        // "SELECT old_text,old_flags FROM `text` WHERE old_id = '"+textId+"'
        // AND old_page = '"+pageId+"' LIMIT 1";
        // For now we run the original one, which works on the data we have
        jsonArray = queryRESTEndpoint(selectTextWithVariables, Integer.toString(textId));

        if (jsonArray.length() == 0) {
            throw new UserAbortException("no such text: " + textId + " for page_id:" + pageId + " page_namespace: " + nameSpace + " page_title:" + pageTitle);
        }
        Article a = null;
        if (!forSelect)
            a = new Article(userText, pageId, jsonArray.getJSONObject(0).getString("old_text"), textId, revisionId);

        return a;
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
