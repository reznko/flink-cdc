/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.postgres.source;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.DecimalType;
import org.apache.flink.cdc.debezium.event.DebeziumSchemaDataTypeInference;

import io.debezium.data.VariableScaleDecimal;
import io.debezium.data.geometry.Geography;
import io.debezium.data.geometry.Geometry;
import io.debezium.data.geometry.Point;
import org.apache.kafka.connect.data.Schema;

/** {@link DataType} inference for PostgresSQL debezium {@link Schema}. */
@Internal
public class PostgresSchemaDataTypeInference extends DebeziumSchemaDataTypeInference {

    private static final long serialVersionUID = 1L;

    @Override
    protected DataType inferStruct(Object value, Schema schema) {
        // the Geometry datatype in PostgresSQL will be converted to
        // a String with Json format
        if (Point.LOGICAL_NAME.equals(schema.name())
                || Geography.LOGICAL_NAME.equals(schema.name())
                || Geometry.LOGICAL_NAME.equals(schema.name())) {
            return DataTypes.STRING();
        } else if (VariableScaleDecimal.LOGICAL_NAME.equals(schema.name())) {
            // NUMERIC / DECIMAL columns declared without precision and scale are
            // emitted by Debezium (decimal.handling.mode=precise) as a
            // VariableScaleDecimal struct. PostgresTypeUtils declares such columns
            // as DECIMAL(38, 0) in the table schema, while the default inference
            // derives a per-value precision and scale. The two layouts of
            // BinaryRecordData differ (compact long vs. variable-length bytes), so
            // records would be written with a different layout than readers of the
            // declared schema expect. Pin the inferred type to the declared one.
            return DataTypes.DECIMAL(DecimalType.MAX_PRECISION, DecimalType.DEFAULT_SCALE);
        } else {
            return super.inferStruct(value, schema);
        }
    }
}
