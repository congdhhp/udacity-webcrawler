package com.udacity.webcrawler.profiler;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Objects;

/**
 * A method interceptor that checks whether {@link Method}s are annotated with the {@link Profiled}
 * annotation. If they are, the method interceptor records how long the method invocation took.
 */
final class ProfilingMethodInterceptor implements InvocationHandler {

  private final Clock clock;
  private final Object delegate;
  private final ProfilingState state;

  // TODO: You will need to add more instance fields and constructor arguments to this class.
  ProfilingMethodInterceptor(Clock clock, Object delegate, ProfilingState state) {
    this.clock = Objects.requireNonNull(clock);
    this.delegate = Objects.requireNonNull(delegate);
    this.state = Objects.requireNonNull(state);
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    // TODO: This method interceptor should inspect the called method to see if it is a profiled
    //       method. For profiled methods, the interceptor should record the start time, then
    //       invoke the method using the object that is being profiled. Finally, for profiled
    //       methods, the interceptor should record how long the method call took, using the
    //       ProfilingState methods.

    // Determine if the method should be profiled
    if (!method.isAnnotationPresent(Profiled.class)) {
      // Directly invoke the method if it is not profiled
      return method.invoke(delegate, args);
    }

    // If the method is profiled, proceed with recording execution time
    Instant start = clock.instant();
    try {
      return method.invoke(delegate, args); // Invoke the target method
    } catch (InvocationTargetException ex) {
      // Re-throw the cause of the InvocationTargetException to the caller
      throw ex.getCause();
    } finally {
      // Compute the duration and record it in the profiling state
      Instant end = clock.instant();
      Duration duration = Duration.between(start, end);
      state.record(delegate.getClass(), method, duration); // Record the profiling data
    }
  }
}
