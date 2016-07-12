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

/**
 * A simple bean used by {@link QueryableStateProxy} when responding to
 * {@link QueryableStateProxy#byKey(String, String)}.
 */
public class KeyValueBean {

  private String key;
  private Long value;

  public KeyValueBean() {}

  public KeyValueBean(final String key, final Long value) {
    this.key = key;
    this.value = value;
  }

  public String getKey() {

    return key;
  }

  public void setKey(final String key) {
    this.key = key;
  }

  public Long getValue() {
    return value;
  }

  public void setValue(final Long value) {
    this.value = value;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final KeyValueBean that = (KeyValueBean) o;

    if (key != null ? !key.equals(that.key) : that.key != null) {
      return false;
    }
    return value != null ? value.equals(that.value) : that.value == null;

  }

  @Override
  public int hashCode() {
    int result = key != null ? key.hashCode() : 0;
    result = 31 * result + (value != null ? value.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "KeyValueBean{" +
           "key='" + key + '\'' +
           ", value=" + value +
           '}';
  }
}
