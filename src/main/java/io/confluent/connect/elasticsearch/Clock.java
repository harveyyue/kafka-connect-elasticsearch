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

import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * An abstraction for a clock.
 *
 * @author Randall Hauch
 */
public interface Clock {

  /**
   * The {@link Clock} instance that uses the {@link System} methods.
   */
  Clock SYSTEM = new Clock() {
    @Override
    public long currentTimeInMillis() {
      return System.currentTimeMillis();
    }

    @Override
    public long currentTimeInNanos() {
      return System.nanoTime();
    }

    @Override
    public Instant currentTimeAsInstant() {
      return Instant.now();
    }
  };

  /**
   * Get the {@link Clock} instance that uses the {@link System} methods.
   *
   * @return the system clock; never null
   */
  static Clock system() {
    return SYSTEM;
  }

  default Instant currentTime() {
    return Instant.ofEpochMilli(currentTimeInMillis());
  }

  /**
   * Get the current time in nanoseconds.
   *
   * @return the current time in nanoseconds.
   */
  default long currentTimeInNanos() {
    return currentTimeInMillis() * 1000000L;
  }

  /**
   * Get the current time in microseconds.
   *
   * @return the current time in microseconds.
   */
  default long currentTimeInMicros() {
    return TimeUnit.MICROSECONDS.convert(currentTimeInMillis(), TimeUnit.MILLISECONDS);
  }

  /**
   * Get the current time as an instant
   *
   * @return the current time as an instant.
   */
  default Instant currentTimeAsInstant() {
    return Instant.ofEpochMilli(currentTimeInMillis());
  }

  /**
   * Get the current time in milliseconds.
   *
   * @return the current time in milliseconds.
   */
  long currentTimeInMillis();

}
