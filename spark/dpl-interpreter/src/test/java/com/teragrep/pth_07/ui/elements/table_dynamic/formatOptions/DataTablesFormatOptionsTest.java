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
package com.teragrep.pth_07.ui.elements.table_dynamic.formatOptions;

import com.teragrep.zep_01.interpreter.InterpreterException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DataTablesFormatOptionsTest {

    final int draw = 3;
    final int start = 3;
    final int length = 2;
    final String searchString = "";

    @Test
    void testDraw() {
        final HashMap<String, String> optionsMap = new HashMap<>();
        optionsMap.put("draw",Integer.toString(draw));
        final DataTablesFormatOptions options = new DataTablesFormatOptions(optionsMap);
        Assertions.assertEquals(draw,Assertions.assertDoesNotThrow(()->options.draw()));
    }

    @Test
    void testSearch() {
        final HashMap<String, String> optionsMap = new HashMap<>();
        optionsMap.put("search",searchString);
        final DataTablesFormatOptions options = new DataTablesFormatOptions(optionsMap);
        Assertions.assertEquals(searchString,Assertions.assertDoesNotThrow(()->options.search()));
    }

    @Test
    void testStart() {
        final HashMap<String, String> optionsMap = new HashMap<>();
        optionsMap.put("start",Integer.toString(start));
        final DataTablesFormatOptions options = new DataTablesFormatOptions(optionsMap);
        Assertions.assertEquals(start,Assertions.assertDoesNotThrow(()->options.start()));
    }

    @Test
    void testLength() {
        final HashMap<String, String> optionsMap = new HashMap<>();
        optionsMap.put("length",Integer.toString(length));
        final DataTablesFormatOptions options = new DataTablesFormatOptions(optionsMap);
        Assertions.assertEquals(length,Assertions.assertDoesNotThrow(()->options.length()));
    }

    @Test
    void testMissingfields(){
        final HashMap<String, String> optionsMap = new HashMap<>();
        optionsMap.put("draw",Integer.toString(draw));
        optionsMap.put("start",Integer.toString(start));
        optionsMap.put("search",searchString);
        final DataTablesFormatOptions options = new DataTablesFormatOptions(optionsMap);
        Assertions.assertThrows(InterpreterException.class,()->options.length());


        final HashMap<String, String> emptyMap = new HashMap<>();
        final DataTablesFormatOptions emptyOptions = new DataTablesFormatOptions(emptyMap);
        Assertions.assertThrows(InterpreterException.class,()->emptyOptions.draw());
        Assertions.assertThrows(InterpreterException.class,()->emptyOptions.start());
        Assertions.assertThrows(InterpreterException.class,()->emptyOptions.length());
        Assertions.assertThrows(InterpreterException.class,()->emptyOptions.search());


    }

}