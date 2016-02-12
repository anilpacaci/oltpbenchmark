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

package com.oltpbenchmark.benchmarks.rest;

import static com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig.terminalPrefix;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.MediaType;

import org.apache.log4j.Logger;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.rest.procedures.RESTProcedure;
import com.oltpbenchmark.benchmarks.rest.procedures.RESTStockLevel;
import com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig;
import com.oltpbenchmark.benchmarks.tpcc.procedures.NewOrder;
import com.oltpbenchmark.util.SimpleSystemPrinter;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource.Builder;

public class RESTBenchmark extends BenchmarkModule {
    private static final Logger LOG = Logger.getLogger(RESTBenchmark.class);

    public RESTBenchmark(WorkloadConfiguration workConf) {
        super("rest", workConf, true);
    }

    @Override
    protected Package getProcedurePackageImpl() {
        return (RESTStockLevel.class.getPackage());
    }

    /**
     * @param Bool
     */
    @Override
    protected List<Worker> makeWorkersImpl(boolean verbose) throws IOException {
        // HACK: Turn off terminal messages
        jTPCCConfig.TERMINAL_MESSAGES = false;
        ArrayList<Worker> workers = new ArrayList<Worker>();

        try {
            List<RESTWorker> terminals = createTerminals();
            workers.addAll(terminals);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return workers;
    }

    protected Builder makeRestConnection(long terminalID) {
        Client client = new Client();
        String path = workConf.getDBConnection();
        // TODO: @anilpacaci Kronos needs this to differentiate different threads
        path = path + '/' + terminalID;
        System.out.println(path);
        Builder builder = client.resource(path).accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON);
        return builder;
    }

    @Override
    // XXX REST Loader is not needed, database can be populated by JDBC loader
    protected Loader makeLoaderImpl(Connection conn) throws SQLException {

        return null;
        // return new TPCCLoader(this, conn);
    }

    protected ArrayList<RESTWorker> createTerminals() throws SQLException {

        RESTWorker[] terminals = new RESTWorker[workConf.getTerminals()];

        int numWarehouses = (int) workConf.getScaleFactor();// tpccConf.getNumWarehouses();
        int numTerminals = workConf.getTerminals();
        assert (numTerminals >= numWarehouses) : String.format("Insufficient number of terminals '%d' [numWarehouses=%d]", numTerminals, numWarehouses);

        String[] terminalNames = new String[numTerminals];
        // TODO: This is currently broken: fix it!
        int warehouseOffset = Integer.getInteger("warehouseOffset", 1);
        assert warehouseOffset == 1;

        // We distribute terminals evenly across the warehouses
        // Eg. if there are 10 terminals across 7 warehouses, they
        // are distributed as
        // 1, 1, 2, 1, 2, 1, 2
        
        //TODO @anilpacaci: It doesn't care above calculation. Every terminal can go to every warehouse, that simple
        // this code is inside RESTWorker
        final double terminalsPerWarehouse = (double) numTerminals / numWarehouses;
        assert terminalsPerWarehouse >= 1;
        for (int w = 0; w < numWarehouses; w++) {
            // Compute the number of terminals in *this* warehouse
            int lowerTerminalId = (int) (w * terminalsPerWarehouse);
            int upperTerminalId = (int) ((w + 1) * terminalsPerWarehouse);
            // protect against double rounding errors
            int w_id = w + 1;
            if (w_id == numWarehouses)
                upperTerminalId = numTerminals;
            int numWarehouseTerminals = upperTerminalId - lowerTerminalId;

            LOG.info(String.format("w_id %d = %d terminals [lower=%d / upper%d]", w_id, numWarehouseTerminals, lowerTerminalId, upperTerminalId));

            final double districtsPerTerminal = jTPCCConfig.configDistPerWhse / (double) numWarehouseTerminals;
            assert districtsPerTerminal >= 1 : String.format("Too many terminals [districtsPerTerminal=%.2f, numWarehouseTerminals=%d]", districtsPerTerminal, numWarehouseTerminals);
            for (int terminalId = 0; terminalId < numWarehouseTerminals; terminalId++) {
                int lowerDistrictId = (int) (terminalId * districtsPerTerminal);
                int upperDistrictId = (int) ((terminalId + 1) * districtsPerTerminal);
                if (terminalId + 1 == numWarehouseTerminals) {
                    upperDistrictId = jTPCCConfig.configDistPerWhse;
                }
                lowerDistrictId += 1;

                String terminalName = terminalPrefix + "w" + w_id + "d" + lowerDistrictId + "-" + upperDistrictId;
                // TODO: @anilpacaci : I use upper terminalID as the terminalID (which Kronos need to seperate different threads)
                RESTWorker terminal = new RESTWorker(workConf, (long) upperTerminalId, terminalName, w_id, lowerDistrictId, upperDistrictId, this, new SimpleSystemPrinter(null), new SimpleSystemPrinter(System.err), numWarehouses);
                terminals[lowerTerminalId + terminalId] = terminal;
                terminalNames[lowerTerminalId + terminalId] = terminalName;
            }

        }
        assert terminals[terminals.length - 1] != null;

        ArrayList<RESTWorker> ret = new ArrayList<RESTWorker>();
        for (RESTWorker w : terminals)
            ret.add(w);
        return ret;
    }

}
