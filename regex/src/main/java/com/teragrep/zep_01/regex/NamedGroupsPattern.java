/*
 * Regex interpreter for Teragrep
 * Copyright (C) 2026 Suomen Kanuuna Oy
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
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
package com.teragrep.zep_01.regex;

import com.teragrep.zep_01.regex.captureGroup.Text;
import com.teragrep.zep_01.regex.captureGroup.TextImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NamedGroupsPattern {

    private static final Logger LOGGER = LoggerFactory.getLogger(NamedGroupsPattern.class);
    private final Pattern pattern;

    public NamedGroupsPattern(Pattern pattern) {
        this.pattern = pattern;
    }

    public Map<Integer, Text> namedGroupIndexes() throws RegexInterpreterException {
        final Method namedGroupsMethod;
        try {
            // java 11 does not have namedGroups as public so reflection is needed
            namedGroupsMethod = Pattern.class.getDeclaredMethod("namedGroups");
        }
        catch (NoSuchMethodException e) {
            LOGGER.error("reflection error getDeclaredMethod", e);
            throw new RegexInterpreterException("reflection error getDeclaredMethod", e);
        }

        namedGroupsMethod.setAccessible(true);

        final Map<String, Integer> groupMap;
        try {
            @SuppressWarnings("unchecked")
            final Map<String, Integer> groupMapLocal = (Map<String, Integer>) namedGroupsMethod.invoke(pattern);
            groupMap = groupMapLocal;
        }
        catch (InvocationTargetException | IllegalAccessException e) {
            LOGGER.error("reflection error invoke", e);
            throw new RegexInterpreterException("reflection error invoke", e);
        }

        // invert the map so group index points to name
        final Map<Integer, Text> indexToName = new HashMap<>();
        for (Map.Entry<String, Integer> entry : groupMap.entrySet()) {
            indexToName.put(entry.getValue(), new TextImpl(entry.getKey()));
        }

        return Collections.unmodifiableMap(indexToName);
    }

    Matcher matcher(final String content) {
        return pattern.matcher(content);
    }

    @Override
    public String toString() {
        return pattern.toString();
    }

}
