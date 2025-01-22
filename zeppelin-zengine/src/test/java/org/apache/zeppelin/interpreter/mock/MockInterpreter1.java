/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.zeppelin.interpreter.mock;

import org.apache.zeppelin.interpreter.*;
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion;
import com.teragrep.zep_04.interpreter.Code;
import com.teragrep.zep_04.interpreter.FormType;
import com.teragrep.zep_04.interpreter.InterpreterContext;
import com.teragrep.zep_04.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class MockInterpreter1 extends AbstractInterpreter {

	private static AtomicInteger IdGenerator = new AtomicInteger();

	private int object_id;
	private String pid;
	Map<String, Object> vars = new HashMap<>();

	public MockInterpreter1(Properties property) {
		super(property);
		this.object_id = IdGenerator.getAndIncrement();
		this.pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
	}

	boolean open;


	@Override
	public void open() {
		open = true;
	}

	@Override
	public void close() {
		open = false;
	}


	public boolean isOpen() {
		return open;
	}

	@Override
	public InterpreterResultImpl interpret(String st, InterpreterContext context) {
		InterpreterResultImpl result;
		st = st.trim();
		if ("getId".equals(st)) {
			// get unique id of this interpreter instance
			result = new InterpreterResultImpl(Code.SUCCESS, "" + this.object_id + "-" + this.pid);
		} else if (st.startsWith("sleep")) {
			try {
				Thread.sleep(Integer.parseInt(st.split(" ")[1]));
			} catch (InterruptedException e) {
				// nothing to do
			}
			result = new InterpreterResultImpl(Code.SUCCESS, "repl1: " + st);
		} else {
			result = new InterpreterResultImpl(Code.SUCCESS, "repl1: " + st);
		}

		if (context.getResourcePool() != null) {
			context.getResourcePool().put(context.getNoteId(), context.getParagraphId(), "result", result);
		}

		return result;
	}

	@Override
	public void cancel(InterpreterContext context) {
	}

	@Override
	public FormType getFormType() {
		return FormType.SIMPLE;
	}

	@Override
	public int getProgress(InterpreterContext context) {
		return 0;
	}

	@Override
	public Scheduler getScheduler() {
		return SchedulerFactory.singleton().createOrGetFIFOScheduler("test_"+this.hashCode());
	}

	@Override
	public List<InterpreterCompletion> completion(String buf, int cursor,
			InterpreterContext interpreterContext
    ) {
		return null;
	}
}
