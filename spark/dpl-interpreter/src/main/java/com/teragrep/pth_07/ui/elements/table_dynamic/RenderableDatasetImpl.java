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
package com.teragrep.pth_07.ui.elements.table_dynamic;

import com.teragrep.pth_07.ui.elements.table_dynamic.formats.AvailableFormat;
import com.teragrep.pth_07.ui.elements.table_dynamic.formats.RenderFormat;
import com.teragrep.pth_07.ui.elements.table_dynamic.formats.RenderFormatStub;
import com.teragrep.pth_07.ui.elements.table_dynamic.formats.UIOption;
import jakarta.json.JsonObject;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public final class RenderableDatasetImpl implements RenderableDataset {

    private static final RenderFormat renderFormatStub = new RenderFormatStub();
    private final List<AvailableFormat> availableFormats;
    private final Dataset<Row> rowDataset;

    RenderableDatasetImpl(final List<AvailableFormat> availableFormats, Dataset<Row> rowDataset) {
        this.availableFormats = availableFormats;
        this.rowDataset = rowDataset;
    }

    @Override
    public RenderFormat toRenderFormat(UIOption uiOption) {
        RenderFormat rv = renderFormatStub;
        for (AvailableFormat availableFormat : this.availableFormats) {
            rv = availableFormat.asRenderFormat(uiOption, rowDataset);
            if (!rv.isStub()) {
                break;
            }
        }
        return rv;
    }

    @Override
    public void persist(){
        rowDataset.persist(StorageLevel.MEMORY_AND_DISK());
    }

    @Override
    public void unpersist(){
        rowDataset.unpersist();
    }

    @Override
    public boolean isStub() {
        return false;
    }

}