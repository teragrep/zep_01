package org.apache.zeppelin.interpreter;

import org.apache.zeppelin.interpreter.xref.InterpreterContext;

import java.util.concurrent.ConcurrentHashMap;

public class InterpreterContextStore {

    private static final ThreadLocal<InterpreterContext> threadIC = new ThreadLocal<>();
    private static final ConcurrentHashMap<Thread, InterpreterContext> allContexts = new ConcurrentHashMap<>();

    public static InterpreterContext get() {
      return threadIC.get();
    }

    public static void set(InterpreterContext ic) {
      threadIC.set(ic);
      allContexts.put(Thread.currentThread(), ic);
    }

    public static void remove() {
      threadIC.remove();
      allContexts.remove(Thread.currentThread());
    }

    public static ConcurrentHashMap<Thread, InterpreterContext> getAllContexts() {
      return allContexts;
    }

}
