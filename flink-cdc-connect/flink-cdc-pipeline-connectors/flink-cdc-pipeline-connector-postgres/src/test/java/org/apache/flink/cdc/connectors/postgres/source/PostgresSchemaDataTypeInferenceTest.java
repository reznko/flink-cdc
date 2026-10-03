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

import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.DecimalType;

import io.debezium.data.VariableScaleDecimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PostgresSchemaDataTypeInference}. */
class PostgresSchemaDataTypeInferenceTest {

    private static final DataType DECLARED_UNCONSTRAINED_NUMERIC_TYPE =
            DataTypes.DECIMAL(DecimalType.MAX_PRECISION, DecimalType.DEFAULT_SCALE);

    private final PostgresSchemaDataTypeInference inference = new PostgresSchemaDataTypeInference();

    @Test
    void testVariableScaleDecimalIsPinnedToDeclaredType() {
        Schema schema = VariableScaleDecimal.schema();

        // The inferred type must not depend on the value, otherwise records would be written
        // with a different binary layout than the one declared in the table schema.
        for (String value :
                new String[] {
                    "0",
                    "123",
                    "-123",
                    "987.65",
                    "12345678901234567890123456789",
                    "-12345678901234567890123456789",
                    "0.000001"
                }) {
            Struct struct = VariableScaleDecimal.fromLogical(schema, new BigDecimal(value));
            assertThat(inference.infer(struct, schema))
                    .as("inferred type for NUMERIC value %s", value)
                    .isEqualTo(DECLARED_UNCONSTRAINED_NUMERIC_TYPE.notNull());
        }
    }

    @Test
    void testNullVariableScaleDecimalIsPinnedToDeclaredType() {
        Schema optionalSchema = VariableScaleDecimal.optionalSchema();

        assertThat(inference.infer(null, optionalSchema))
                .isEqualTo(DECLARED_UNCONSTRAINED_NUMERIC_TYPE);
        assertThat(inference.infer(VariableScaleDecimal.ZERO, optionalSchema))
                .isEqualTo(DECLARED_UNCONSTRAINED_NUMERIC_TYPE);
    }
}
