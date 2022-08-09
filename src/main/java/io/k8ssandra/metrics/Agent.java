package io.k8ssandra.metrics;

import java.lang.instrument.Instrumentation;

import io.k8ssandra.metrics.interceptors.CassandraDaemonInterceptor;
import net.bytebuddy.agent.builder.AgentBuilder;

public class Agent {
    public static void premain(String arguments, Instrumentation instrumentation) {
        new AgentBuilder.Default()
                .type(CassandraDaemonInterceptor.type())
                .transform(CassandraDaemonInterceptor.transformer())
                .installOn(instrumentation);
    }
}
