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

package com.oltpbenchmark.benchmarks.wikirest;

import java.net.UnknownHostException;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionGenerator;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.wikipedia.procedures.AddWatchList;
import com.oltpbenchmark.benchmarks.wikipedia.procedures.GetPageAnonymous;
import com.oltpbenchmark.benchmarks.wikipedia.procedures.GetPageAuthenticated;
import com.oltpbenchmark.benchmarks.wikipedia.procedures.RemoveWatchList;
import com.oltpbenchmark.benchmarks.wikipedia.procedures.UpdatePage;
import com.oltpbenchmark.benchmarks.wikipedia.util.Article;
import com.oltpbenchmark.benchmarks.wikipedia.util.WikipediaOperation;
import com.oltpbenchmark.benchmarks.wikirest.procedures.GetPageAnonymousRest;
import com.oltpbenchmark.benchmarks.wikirest.procedures.GetPageAuthenticatedRest;
import com.oltpbenchmark.types.TransactionStatus;
import com.oltpbenchmark.util.TextGenerator;
import com.sun.jersey.api.client.WebResource.Builder;
import com.oltpbenchmark.util.RandomDistribution.Flat;

public class WikirestWorker extends Worker {
    private static final Logger LOG = Logger.getLogger(WikirestWorker.class);
    private final TransactionGenerator<WikipediaOperation> generator;

    final Flat usersRng;
    final int num_users;
    private Builder builder;

    public WikirestWorker(int id, WikirestBenchmark benchmarkModule, TransactionGenerator<WikipediaOperation> generator) {
        super(benchmarkModule, id);
        this.builder = benchmarkModule.makeRestConnection();
        this.generator = generator;
        this.num_users = (int) Math.round(WikipediaConstants.USERS * this.getWorkloadConfiguration().getScaleFactor());
        this.usersRng = new Flat(rng(), 1, this.num_users);
    }

    private String generateUserIP() {
        return String.format("%d.%d.%d.%d", rng().nextInt(255) + 1, rng().nextInt(256), rng().nextInt(256), rng().nextInt(256));
    }

    @Override
    protected TransactionStatus executeWork(TransactionType nextTransaction) throws UserAbortException, SQLException {
        WikipediaOperation t = null;

        Class<? extends Procedure> procClass = nextTransaction.getProcedureClass();
        boolean needUser = procClass.equals(GetPageAuthenticatedRest.class);
        while (t == null) {
            t = this.generator.nextTransaction();
            if (needUser && t.userId == 0) {
                t = null;
            }
        } // WHILE
        assert (t != null);
        if (t.userId != 0)
            t.userId = this.usersRng.nextInt();

        // AddWatchList
        try {

            // GetPageAnonymous
            if (procClass.equals(GetPageAnonymousRest.class)) {
                getPageAnonymous(true, this.generateUserIP(), t.nameSpace, t.pageTitle);
                // LOG.debug("GetPageAnonymous Successful");
            }
            // GetPageAuthenticated
            else if (procClass.equals(GetPageAuthenticatedRest.class)) {
                assert (t.userId > 0);
                getPageAuthenticated(true, this.generateUserIP(), t.userId, t.nameSpace, t.pageTitle);
                // LOG.debug("GetPageAuthenticated Successful");
            }
        } catch (SQLException esql) {
            LOG.error("Caught SQL Exception in WikipediaWorker for procedure" + procClass.getName() + ":" + esql, esql);
            throw esql;
        } /*
           * catch(Exception e) { LOG.error(
           * "caught Exception in WikipediaWorker for procedure "
           * +procClass.getName() +":" + e, e); }
           */ catch (JSONException e) {
            LOG.error("Caught JSON Exception in WikipediaWorker for procedure" + procClass.getName() + ":" + e, e);
            throw new SQLException("Cast from JSONException: " + e.getMessage(), e);
        }
        return (TransactionStatus.SUCCESS);
    }

    /**
     * Implements wikipedia selection of last version of an article (with and
     * without the user being logged in)
     * 
     * @parama userIp contains the user's IP address in dotted quad form for
     *         IP-based access control
     * @param userId
     *            the logged in user's identifer. If negative, it is an
     *            anonymous access.
     * @param nameSpace
     * @param pageTitle
     * @return article (return a Class containing the information we extracted,
     *         useful for the updatePage transaction)
     * @throws SQLException
     * @throws JSONException
     * @throws UserAbortException
     * @throws UnknownHostException
     */
    public Article getPageAnonymous(boolean forSelect, String userIp, int nameSpace, String pageTitle) throws SQLException, UserAbortException, JSONException {
        GetPageAnonymousRest proc = this.getProcedure(GetPageAnonymousRest.class);
        assert (proc != null);
        return proc.run(builder, forSelect, userIp, nameSpace, pageTitle);
    }

    public Article getPageAuthenticated(boolean forSelect, String userIp, int userId, int nameSpace, String pageTitle) throws SQLException, JSONException {
        GetPageAuthenticatedRest proc = this.getProcedure(GetPageAuthenticatedRest.class);
        assert (proc != null);
        return proc.run(builder, forSelect, userIp, userId, nameSpace, pageTitle);
    }

}
