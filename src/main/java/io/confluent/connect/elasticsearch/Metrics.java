/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.elasticsearch;

import org.apache.kafka.common.utils.Sanitizer;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Base for metrics implementations.
 */
public class Metrics {
  private final Logger log = LoggerFactory.getLogger(Metrics.class);

  // Total 1 minute attempting to retry metrics registration in case of errors
  private static final int REGISTRATION_RETRIES = 12;
  private static final Duration REGISTRATION_RETRY_DELAY = Duration.ofSeconds(5);

  private final ObjectName name;
  private volatile boolean registered = false;

  public Metrics(ElasticsearchSinkConnectorConfig esConfig, String contextName) {
    String connectorType = esConfig.getContextName();
    String connectorName = esConfig.getConnectorName();
    String taskId = esConfig.getTaskId();
    this.name = metricName(connectorType, connectorName, contextName, taskId);
  }

  protected ObjectName metricName(String connectorType, String connectorName, String contextName, String taskId) {
    Map<String, String> tags = new LinkedHashMap<>();
    tags.put("context", contextName);
    tags.put("server", connectorName);
    tags.put("task", taskId);
    return metricName(connectorType, tags);
  }

  /**
   * Create a JMX metric name for the given metric.
   *
   * @return the JMX metric name
   */
  protected ObjectName metricName(String connectorType, Map<String, String> tags) {
    final String metricName = "es." + connectorType.toLowerCase() + ":type=connector-metrics,"
        + tags.entrySet().stream()
        .map(e -> e.getKey() + "=" + Sanitizer.jmxSanitize(e.getValue()))
        .collect(Collectors.joining(","));
    try {
      return new ObjectName(metricName);
    } catch (MalformedObjectNameException e) {
      throw new ConnectException("Invalid metric name '" + metricName + "'");
    }
  }

  /**
   * Registers a metrics MBean into the platform MBean server.
   * The method is intentionally synchronized to prevent preemption between registration and unregistration.
   */
  public synchronized void register() {
    try {
      final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
      if (mBeanServer == null) {
        log.info("JMX not supported, bean '{}' not registered", name);
        return;
      }
      // During connector restarts it is possible that Kafka Connect does not manage
      // the lifecycle perfectly. In that case it is possible the old metric MBean is still present.
      // There will be multiple attempts executed to register new MBean.
      for (int attempt = 1; attempt <= REGISTRATION_RETRIES; attempt++) {
        try {
          mBeanServer.registerMBean(this, name);
          break;
        } catch (InstanceAlreadyExistsException e) {
          if (attempt < REGISTRATION_RETRIES) {
            log.warn(
                "Unable to register metrics as an old set with the same name exists, retrying in {} (attempt {} out of {})",
                REGISTRATION_RETRY_DELAY, attempt, REGISTRATION_RETRIES);
            final Metronome metronome = Metronome.sleeper(REGISTRATION_RETRY_DELAY, Clock.system());
            metronome.pause();
          } else {
            log.error("Failed to register metrics MBean, metrics will not be available");
          }
        }
      }
      // If the old metrics MBean is present then the connector will try to unregister it
      // upon shutdown.
      registered = true;
    } catch (JMException | InterruptedException e) {
      throw new RuntimeException("Unable to register the MBean '" + name + "'", e);
    }
  }

  /**
   * Unregisters a metrics MBean from the platform MBean server.
   * The method is intentionally synchronized to prevent preemption between registration and unregistration.
   */
  public synchronized void unregister() {
    if (this.name != null && registered) {
      try {
        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        if (mBeanServer == null) {
          log.debug("JMX not supported, bean '{}' not registered", name);
          return;
        }
        try {
          mBeanServer.unregisterMBean(name);
        } catch (InstanceNotFoundException e) {
          log.info("Unable to unregister metrics MBean '{}' as it was not found", name);
        }
        registered = false;
      } catch (JMException e) {
        throw new RuntimeException("Unable to unregister the MBean '" + name + "'", e);
      }
    }
  }
}
