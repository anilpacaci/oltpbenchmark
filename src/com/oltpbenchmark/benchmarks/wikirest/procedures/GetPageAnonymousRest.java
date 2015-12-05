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

import java.sql.SQLException;

import javax.ws.rs.core.MediaType;

import org.apache.commons.lang3.StringUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.benchmarks.wikipedia.WikipediaConstants;
import com.oltpbenchmark.benchmarks.wikipedia.util.Article;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource.Builder;

public class GetPageAnonymousRest extends Procedure {

    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------

    public final String selectPageWithVariables = "SELECT * FROM " + WikipediaConstants.TABLENAME_PAGE + " WHERE page_namespace = ? AND page_title = '?' LIMIT 1";
    public final String selectPageRestrictionWithVariables = "SELECT * FROM " + WikipediaConstants.TABLENAME_PAGE_RESTRICTIONS + " WHERE pr_page = ?";
    // XXX this is hard for translation
    public final String selectIpBlocksWithVariables = "SELECT * FROM " + WikipediaConstants.TABLENAME_IPBLOCKS + " WHERE ipb_address = '?'";
    public final String selectPageRevisionWithVariables = "SELECT * " + "  FROM " + WikipediaConstants.TABLENAME_PAGE + ", " + WikipediaConstants.TABLENAME_REVISION + " WHERE page_id = rev_page "
            + "   AND rev_page = ? " + "   AND page_id = ? " + "   AND rev_id = page_latest LIMIT 1";
    public final String selectTextWithVariables = "SELECT old_text, old_flags FROM " + WikipediaConstants.TABLENAME_TEXT + " WHERE old_id = ? LIMIT 1";

    private Builder builder;
    private final static String SQL_VARIABLE = "?";

    // -----------------------------------------------------------------
    // RUN
    // -----------------------------------------------------------------

    public Article run(Builder builder, boolean forSelect, String userIp, int pageNamespace, String pageTitle) throws UserAbortException, SQLException, JSONException {
        int param = 1;

        this.builder = builder;

        String selectPage = selectPageWithVariables;

        selectPage = StringUtils.replaceOnce(selectPage, SQL_VARIABLE, Integer.toString(pageNamespace));
        selectPage = StringUtils.replaceOnce(selectPage, SQL_VARIABLE, pageTitle);

        ClientResponse response = getClient().post(ClientResponse.class, selectPage);
        if (response.getClientResponseStatus() != com.sun.jersey.api.client.ClientResponse.Status.OK) {
            throw new SQLException("Query " + selectPage + " encountered an error ");
        }

        JSONArray jsonArray = response.getEntity(JSONArray.class);
        response.close();

        if (jsonArray.length() == 0) {
            String msg = String.format("Invalid Page: Namespace:%d / Title:--%s--", pageNamespace, pageTitle);
            throw new UserAbortException(msg);
        }
        int pageId = jsonArray.getJSONObject(0).optInt("page_id");

        // selectPageRestriction
        String selectPageRestriction = selectPageRestrictionWithVariables;

        selectPageRestriction = StringUtils.replaceOnce(selectPageRestriction, SQL_VARIABLE, Integer.toString(pageId));

        response = getClient().post(ClientResponse.class, selectPageRestriction);
        if (response.getClientResponseStatus() != com.sun.jersey.api.client.ClientResponse.Status.OK) {
            throw new SQLException("Query " + selectPageRestriction + " encountered an error ");
        }

        jsonArray = response.getEntity(JSONArray.class);
        response.close();
        
        if (jsonArray.length() == 0 || !jsonArray.getJSONObject(0).has("pr_page")) {
            String msg = String.format("Invalid Page: ID:%d ", pageId);
            throw new UserAbortException(msg);
        }

        // selectIP Block
        String selectIpBlocks = selectIpBlocksWithVariables;

        selectIpBlocks = StringUtils.replaceOnce(selectIpBlocks, SQL_VARIABLE, userIp);

        response = getClient().post(ClientResponse.class, selectIpBlocks);
        if (response.getClientResponseStatus() != com.sun.jersey.api.client.ClientResponse.Status.OK) {
            throw new SQLException("Query " + selectIpBlocks + " encountered an error ");
        }

        jsonArray = response.getEntity(JSONArray.class);
        response.close();
        
        if (jsonArray.length() == 0 || !jsonArray.getJSONObject(0).has("ipb_id")) {
            String msg = String.format("Invalid IP Block: Ip:%d ", userIp);
            throw new UserAbortException(msg);
        }
        
     // selectPageRevision
        String selectPageRevision = selectPageRevisionWithVariables;

        selectPageRevision = StringUtils.replaceOnce(selectPageRevision, SQL_VARIABLE, Integer.toString(pageId));
        selectPageRevision = StringUtils.replaceOnce(selectPageRevision, SQL_VARIABLE, Integer.toString(pageId));

        response = getClient().post(ClientResponse.class, selectPageRevision);
        if (response.getClientResponseStatus() != com.sun.jersey.api.client.ClientResponse.Status.OK) {
            throw new SQLException("Query " + selectPageRevision + " encountered an error ");
        }

        jsonArray = response.getEntity(JSONArray.class);
        response.close();
        
        if (jsonArray.length() == 0) {
            String msg = String.format("Invalid Page: Namespace:%d / Title:--%s-- / PageId:%d", pageNamespace, pageTitle, pageId);
            throw new UserAbortException(msg);
        }
        int revisionId = jsonArray.getJSONObject(0).optInt("rev_id");
        int textId = jsonArray.getJSONObject(0).optInt("rev_text_id");;
        
        
        // NOTE: the following is our variation of wikipedia... the original did
        // not contain old_page column!
        // sql =
        // "SELECT old_text,old_flags FROM `text` WHERE old_id = '"+textId+"'
        // AND old_page = '"+pageId+"' LIMIT 1";
        // For now we run the original one, which works on the data we have
        
        String selectText = selectTextWithVariables;

        selectText = StringUtils.replaceOnce(selectText, SQL_VARIABLE, Integer.toString(textId));

        response = getClient().post(ClientResponse.class, selectText);
        if (response.getClientResponseStatus() != com.sun.jersey.api.client.ClientResponse.Status.OK) {
            throw new SQLException("Query " + selectText + " encountered an error ");
        }

        jsonArray = response.getEntity(JSONArray.class);
        response.close();
        
        if (jsonArray.length() == 0) {
            String msg = "No such text: " + textId + " for page_id:" + pageId + " page_namespace: " + pageNamespace + " page_title:" + pageTitle;
            throw new UserAbortException(msg);
        }
        
        Article a = null;
        if (!forSelect)
            a = new Article(userIp, pageId, jsonArray.getJSONObject(0).optString("old_text"), textId, revisionId);
        return a;
    }

    private Builder getClient() {
        if (builder == null) {
            new ClientHandlerException("No REST Client, request could not be issued");
        }
        return builder.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON);
    }

}
