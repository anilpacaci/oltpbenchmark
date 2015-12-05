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
import java.util.Random;

import org.codehaus.jettison.json.JSONArray;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.benchmarks.rest.RESTWorker;
import com.sun.jersey.api.client.WebResource.Builder;

public abstract class RESTProcedure extends Procedure {

    public abstract JSONArray run(Builder builder, Random gen, int terminalWarehouseID, int numWarehouses, int terminalDistrictLowerID, int terminalDistrictUpperID, RESTWorker w) throws SQLException;

}
