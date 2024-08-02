/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.flink.action;

import java.util.Optional;

import static org.apache.paimon.flink.action.ActionFactory.printDefaultHelp;

/**
 * Table maintenance actions for Flink.
 *
 * @deprecated Compatible with older versions of usage
 */
@Deprecated
public class FlinkActions {

    // ------------------------------------------------------------------------
    //  Flink run methods
    // ------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        String[] args1 = {
            "mongodb_sync_table",
            "--warehouse",
            "hdfs://mobiocluster/paimon-lake",
            "--database",
            "journey_builder",
            "--table",
            "report",
            "--mongodb_conf",
            "hosts=$MONGODB_HOST",
            "--mongodb_conf",
            "username=$MONGODB_FLINK_USERNAME",
            "--mongodb_conf",
            "password=$MONGODB_FLINK_PASSWORD",
            "--mongodb_conf",
            "database=journey_builder",
            "--mongodb_conf",
            "collection=report",
            "--mongodb_conf",
            "schema.start.mode=parse_column",
            "--mongodb_conf",
            "default.id.generation=false",
            "--mongodb_conf",
            "scan.incremental.snapshot.enabled=true",
            "--table_conf",
            "sink.parallelism=1",
            "--table_conf",
            "bucket=4",
            "--table_conf",
            "write-buffer-spillable=true",
            "--table_conf",
            "num-sorted-run.stop-trigger=2147483647",
            "--table_conf",
            "sort-spill-threshold=10"
        };
        if (args1.length < 1) {
            printDefaultHelp();
            System.exit(1);
        }

        Optional<Action> action = ActionFactory.createAction(args1);

        if (action.isPresent()) {
            action.get().run();
        } else {
            System.exit(1);
        }
    }
}
