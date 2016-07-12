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

import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KafkaStreamsMetadata;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowRange;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("state")
public class QueryableStateProxy {

  private final KafkaStreams streams;
  private Server jettyServer;

  QueryableStateProxy(final KafkaStreams streams) {
    this.streams = streams;
  }

  @GET
  @Path("/keyvalue/{storeName}/{key}")
  @Produces(MediaType.APPLICATION_JSON)
  public KeyValueBean byKey(@PathParam("storeName") final String storeName,
                        @PathParam("key") final String key) {
    final ReadOnlyKeyValueStore<String, Long>
        store =
        streams.store(storeName, QueryableStoreTypes.<String, Long>keyValueStore());
    if (store == null) {
      throw new NotFoundException();
    }
    final Long value = store.get(key);
    if (value == null) {
      throw new NotFoundException();
    }
    return new KeyValueBean(key, value);
  }

  @GET()
  @Path("/keyvalues/{storeName}/all")
  @Produces(MediaType.APPLICATION_JSON)
  public List<KeyValueBean> allForStore(@PathParam("storeName") final String storeName) {
    return rangeForKeyValueStore(storeName, ReadOnlyKeyValueStore::all);
  }


  @GET()
  @Path("/keyvalues/{storeName}/range/{from}/{to}")
  @Produces(MediaType.APPLICATION_JSON)
  public List<KeyValueBean> keyRangeForStore(@PathParam("storeName") final String storeName,
                                             @PathParam("from") final String from,
                                             @PathParam("to") final String to) {
    return rangeForKeyValueStore(storeName, store -> store.range(from, to));
  }


  @GET()
  @Path("/windowed/{storeName}/{key}/{from}/{to}")
  @Produces(MediaType.APPLICATION_JSON)
  public List<KeyValueBean> windowedByKey(@PathParam("storeName") final String storeName,
                                          @PathParam("key") final String key,
                                          @PathParam("from") final Long from,
                                          @PathParam("to") final Long to) {
    final ReadOnlyWindowStore<String, Long>
        store =
        streams.store(storeName, QueryableStoreTypes.<String, Long>windowStore());
    if (store == null) {
      throw new NotFoundException();
    }
    final WindowStoreIterator<Long>
        results =
        store.fetch(key, from, to);
    final List<KeyValueBean> windowResults = new ArrayList<>();
    while (results.hasNext()) {
      final KeyValue<Long, Long> next = results.next();
      windowResults.add(new KeyValueBean(key + "@" + next.key, next.value));
    }
    return windowResults;
  }

  @GET()
  @Path("/windowed/{storeName}/windowrange")
  public WindowRangeBean windowRange(@PathParam("storeName") String storeName) {
    final ReadOnlyWindowStore<String, Long>
        store =
        streams.store(storeName, QueryableStoreTypes.<String, Long>windowStore());
    if (store == null) {
      throw new NotFoundException("store " + storeName + " not found");
    }
    final WindowRange windowRange =
        store.openWindowRange();

    return new WindowRangeBean(windowRange.earliest(), windowRange.latest());
  }

  @GET()
  @Path("/instances")
  @Produces(MediaType.APPLICATION_JSON)
  public List<HostStoreInfo> streamsInstances() {
    final Collection<KafkaStreamsMetadata> instances = streams.allMetadata();
    return mapInstancesToHostStoreInfo(instances);
  }


  @GET()
  @Path("/instances/{storeName}")
  @Produces(MediaType.APPLICATION_JSON)
  public List<HostStoreInfo> streamsInstancesForStore(@PathParam("storeName") String store) {
    final Collection<KafkaStreamsMetadata> instances = streams.allMetadataForStore(store);
    return mapInstancesToHostStoreInfo(instances);
  }

  @GET()
  @Path("/instance/{storeName}/{key}")
  @Produces(MediaType.APPLICATION_JSON)
  public HostStoreInfo streamsInstanceForStoreAndKey(@PathParam("storeName") String store,
                                                     @PathParam("key") String key) {
    final KafkaStreamsMetadata
        instance =
        streams.metadataWithKey(store, key, new StringSerializer());
    if (instance == null) {
      throw new NotFoundException();
    }

    return new HostStoreInfo(instance.hostInfo().host(),
                             instance.hostInfo().port(),
                             instance.stateStoreNames());
  }

  private List<HostStoreInfo> mapInstancesToHostStoreInfo(
      final Collection<KafkaStreamsMetadata> instances) {
    return instances.stream().map(instance -> new HostStoreInfo(instance.hostInfo().host(),
                                                                instance.hostInfo().port(),
                                                                instance.stateStoreNames()))
        .collect(Collectors.toList());
  }

  private List<KeyValueBean> rangeForKeyValueStore(final String storeName,
                                                   final Function<ReadOnlyKeyValueStore<String, Long>,
                                                       KeyValueIterator<String, Long>> rangeFunction) {
    final ReadOnlyKeyValueStore<String, Long>
        store =
        streams.store(storeName, QueryableStoreTypes.keyValueStore());
    if (store == null) {
      throw new NotFoundException();
    }
    final List<KeyValueBean> results = new ArrayList<>();
    final KeyValueIterator<String, Long> range = rangeFunction.apply(store);
    while (range.hasNext()) {
      final KeyValue<String, Long> next = range.next();
      results.add(new KeyValueBean(next.key, next.value));
    }

    return results;
  }

  void start(final int port) throws Exception {
    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
    context.setContextPath("/");

    jettyServer = new Server(port);
    jettyServer.setHandler(context);

    ResourceConfig rc = new ResourceConfig();
    rc.register(this);

    ServletContainer sc = new ServletContainer(rc);
    ServletHolder holder = new ServletHolder(sc);
    context.addServlet(holder, "/*");

    jettyServer.start();
  }

  void stop() throws Exception {
    if (jettyServer != null) {
      jettyServer.stop();
    }
  }

}

