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
package com.teragrep.pth_07.ui.elements.table_dynamic.formats;

import com.teragrep.pth_07.ui.elements.table_dynamic.DTPagination;
import com.teragrep.pth_07.ui.elements.table_dynamic.DTSearch;
import com.teragrep.pth_07.ui.elements.table_dynamic.formatOptions.DataTablesFormatOptions;
import com.teragrep.pth_07.ui.elements.table_dynamic.pojo.Order;
import com.teragrep.zep_01.interpreter.InterpreterException;
import com.teragrep.zep_01.interpreter.InterpreterResult;
import jakarta.json.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Iterator;

import java.io.StringReader;
import java.util.List;

public class DataTablesFormat implements  DatasetFormat{

    private final Dataset<Row> dataset;
    private final DataTablesFormatOptions options;
    private static final Logger LOGGER = LoggerFactory.getLogger(DataTablesFormat.class);

    public DataTablesFormat(Dataset<Row> dataset, DataTablesFormatOptions options){
        this.dataset = dataset;
        this.options = options;
    }
    public JsonObject format() throws InterpreterException{
            if(dataset == null){
                throw new InterpreterException("Attempting to draw an empty dataset!");
            }
            try{
                List<String> datasetAsJson = dataset.toJSON().collectAsList();
                DTSearch dtSearch = new DTSearch(datasetAsJson);
                List<Order> currentOrder = null;

                // searching
                List<String> searchedList = dtSearch.search(options.search());

                // TODO ordering
                //DTOrder dtOrder = new DTOrder(searchedList);
                //List<String> orderedlist = dtOrder.order(searchedList, currentOrder);
                List<String> orderedlist = searchedList;

                // pagination
                DTPagination dtPagination = new DTPagination(orderedlist);
                List<String> paginatedList = dtPagination.paginate(options.length(), options.start());

                // ui formatting
                JsonArray formated;
                JsonArrayBuilder arrayBuilder = Json.createArrayBuilder();
                for (String row : paginatedList) {
                    JsonReader reader = Json.createReader(new StringReader(row));
                    JsonObject line = reader.readObject();
                    arrayBuilder.add(line);
                    reader.close();
                }
                formated = arrayBuilder.build();

                JsonArrayBuilder builder = Json.createArrayBuilder();
                Iterator<StructField> it = dataset.schema().iterator();
                while(it.hasNext()) {
                    StructField column = it.next();
                    builder.add(column.name());
                }
                final JsonArray schemaHeadersAsJSON = builder.build();

                LogicalPlan plan = dataset.queryExecution().logical();
                boolean aggsUsed;
                if (plan instanceof Aggregate) {
                    aggsUsed = true;
                }
                else {
                    aggsUsed = false;
                }

                int recordsTotal = datasetAsJson.size();
                int recordsFiltered = searchedList.size();
                JsonObjectBuilder dataBuilder = Json.createObjectBuilder();
                dataBuilder.add("headers",schemaHeadersAsJSON);
                dataBuilder.add("data", formated);
                dataBuilder.add("draw", options.draw());
                dataBuilder.add("recordsTotal", recordsTotal);
                dataBuilder.add("recordsFiltered", recordsFiltered);
                JsonObject data = dataBuilder.build();
                JsonObject json = Json.createObjectBuilder().add("data",data).add("isAggregated",aggsUsed).build();
                return json;
            }catch(JsonException|IllegalStateException e){
                LOGGER.error(e.toString());
                throw new InterpreterException("Failed to format dataset into DataTables format");
            }
    }

    @Override
    public String type(){
        return InterpreterResult.Type.DATATABLES.label;
    }
}
