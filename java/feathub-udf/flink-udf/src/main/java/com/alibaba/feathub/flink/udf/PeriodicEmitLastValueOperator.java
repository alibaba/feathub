/*
 * Copyright 2022 The FeatHub Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.feathub.flink.udf;

import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;

/** An operator that periodically emits the last value it has received from upstream operator. */
public class PeriodicEmitLastValueOperator extends AbstractStreamOperator<Row>
        implements OneInputStreamOperator<Row, Row>, Serializable, BoundedOneInput {
    private final long emitIntervalMs;
    @Nullable private Row lastValue;
    private ListState<Row> lastValueState;

    public PeriodicEmitLastValueOperator(Double emitIntervalSec) {
        this.emitIntervalMs = (long) (emitIntervalSec * 1000);
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        lastValueState =
                context.getOperatorStateStore()
                        .getListState(new ListStateDescriptor<>("lastValueState", Row.class));
        Iterator<Row> iterator = lastValueState.get().iterator();
        if (iterator.hasNext()) {
            lastValue = iterator.next();
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        if (lastValue != null) {
            lastValueState.update(Collections.singletonList(lastValue));
        }
    }

    @Override
    public void processElement(StreamRecord<Row> element) {
        if (lastValue == null) {
            getProcessingTimeService()
                    .registerTimer(
                            getProcessingTimeService().getCurrentProcessingTime() + emitIntervalMs,
                            new EmitLastValueCallback());
        }
        lastValue = element.getValue();
    }

    @Override
    public void endInput() {
        if (lastValue != null) {
            output.collect(new StreamRecord<>(lastValue));
        }
    }

    private class EmitLastValueCallback implements ProcessingTimeService.ProcessingTimeCallback {
        @Override
        public void onProcessingTime(long timestamp) {
            output.collect(new StreamRecord<>(lastValue));
            getProcessingTimeService().registerTimer(timestamp + emitIntervalMs, this);
        }
    }
}
