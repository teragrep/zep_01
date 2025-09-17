/*
 * Teragrep DPL Spark Integration PTH-07
 * Copyright (C) 2022  Suomen Kanuuna Oy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://github.com/teragrep/teragrep/blob/main/LICENSE>.
 *
 *
 * Additional permission under GNU Affero General Public License version 3
 * section 7
 *
 * If you modify this Program, or any covered work, by linking or combining it
 * with other code, such other code is not for that reason alone subject to any
 * of the requirements of the GNU Affero GPL version 3 as long as this Program
 * is the same Program as licensed from Suomen Kanuuna Oy without any additional
 * modifications.
 *
 * Supplemented terms under GNU Affero General Public License version 3
 * section 7
 *
 * Origin of the software must be attributed to Suomen Kanuuna Oy. Any modified
 * versions must be marked as "Modified version of" The Program.
 *
 * Names of the licensors and authors may not be used for publicity purposes.
 *
 * No rights are granted for use of trade names, trademarks, or service marks
 * which are in The Program if any.
 *
 * Licensee must indemnify licensors and authors for any liability that these
 * contractual assumptions impose on licensors and authors.
 *
 * To the extent this program is licensed as part of the Commercial versions of
 * Teragrep, the applicable Commercial License may apply to this file if you as
 * a licensee so wish it.
 */
package com.teragrep.pth_07.ui.elements;

import com.teragrep.zep_01.display.AngularObject;
import com.teragrep.zep_01.display.AngularObjectWatcher;
import com.teragrep.zep_01.interpreter.InterpreterContext;
import jakarta.json.Json;
import jakarta.json.JsonObject;

import java.util.Map;

public class PerformanceIndicator extends AbstractUserInterfaceElement {

    private final AngularObject<String> batchMsg;
    private String message;

    public PerformanceIndicator(InterpreterContext interpreterContext) {
        super(interpreterContext);

        AngularObjectWatcher angularObjectWatcher = new AngularObjectWatcher(getInterpreterContext()) {
            @Override
            public void watch(Object o, Object o1, InterpreterContext interpreterContext) {
                if(LOGGER.isTraceEnabled()) {
                    LOGGER.trace("batchMsg <- {}", o.toString());
                    LOGGER.trace("batchMsg -> {}", o1.toString());
                }
            }
        };

        batchMsg = getInterpreterContext().getAngularObjectRegistry().add(
                "batchMsg",
                "",
                getInterpreterContext().getNoteId(),
                getInterpreterContext().getParagraphId(),
                true
        );
        batchMsg.addWatcher(angularObjectWatcher);

    }

    @Override
    protected void draw() {
        batchMsg.set(message);
    }

    @Override
    public void emit() {
        batchMsg.emit();
    }

    public void setPerformanceData(long numInputRows, long batchId, double processedRowsPerSecond, Map<String, String> currentMetrics) {
        final StringBuilder newMessage = new StringBuilder();

        newMessage.append("Full table input rows read from archive: ").append(numInputRows).append(" ");
        newMessage.append("during batchId: ").append(batchId).append(" ");
        newMessage.append("with avg EPS: ").append(processedRowsPerSecond).append("\n");

        if (!currentMetrics.isEmpty()) {
            newMessage.append("Metrics:\n");
            for (final Map.Entry<String, String> metric : currentMetrics.entrySet()) {
                newMessage.append(metric.getKey()).append(": ").append(metric.getValue()).append("\n");
            }
        }

        message = newMessage.toString();

        draw();
    }
}