/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.  You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.confluent.examples.streams.queryablestate;

import java.util.Set;

/**
 * A simple bean that can be JSON serialized via Jersey. Represents a KafkaStreams instance
 * that has a set of state stores. See {@link QueryableStateProxy} for how it is used.
 */
public class HostStoreInfo {

  private String host;
  private int port;
  private Set<String> storeNames;

  public HostStoreInfo(){}

  public HostStoreInfo(final String host, final int port, final Set<String> storeNames) {
    this.host = host;
    this.port = port;
    this.storeNames = storeNames;
  }

  public String getHost() {
    return host;
  }

  public void setHost(final String host) {
    this.host = host;
  }

  public int getPort() {
    return port;
  }

  public void setPort(final int port) {
    this.port = port;
  }

  public Set<String> getStoreNames() {
    return storeNames;
  }

  public void setStoreNames(final Set<String> storeNames) {
    this.storeNames = storeNames;
  }

  @Override
  public String toString() {
    return "HostStoreInfo{" +
           "host='" + host + '\'' +
           ", port=" + port +
           ", storeNames=" + storeNames +
           '}';
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final HostStoreInfo that = (HostStoreInfo) o;

    if (port != that.port) {
      return false;
    }
    if (host != null ? !host.equals(that.host) : that.host != null) {
      return false;
    }
    return storeNames != null ? storeNames.equals(that.storeNames) : that.storeNames == null;

  }

  @Override
  public int hashCode() {
    int result = host != null ? host.hashCode() : 0;
    result = 31 * result + port;
    result = 31 * result + (storeNames != null ? storeNames.hashCode() : 0);
    return result;
  }
}
