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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SkippablePrompt {

    private static final Logger LOGGER = LoggerFactory.getLogger(SkippablePrompt.class);

    private final String prompt;
    private final int newlineIndex;

    public SkippablePrompt(String prompt) {
        this(prompt, prompt.indexOf('\n'));
    }

    public SkippablePrompt(String prompt, int newLineIndex) {
        this.prompt = prompt;
        this.newlineIndex = newLineIndex;
    }

    public String skipFirstLine() throws RegexInterpreterException {
        LOGGER.trace("Interpreting prompt <[{}]>", prompt);

        if (newlineIndex == -1) {
            throw new RegexInterpreterException("unrecognized prompt, please newline after interpreter declaration and use regex on the first line and content on subsequent line(s)");
        }
        String omitted = prompt.substring(0, newlineIndex);
        LOGGER.trace("omitting <[{}]>",  omitted);

        return prompt.substring(newlineIndex + 1);
    }
}
