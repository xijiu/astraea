/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.app.homework;

import com.beust.jcommander.Parameter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.astraea.app.argument.DataSizeField;
import org.astraea.app.argument.StringListField;
import org.astraea.common.DataSize;
import org.astraea.common.DataUnit;
import org.astraea.common.admin.AdminConfigs;

public class BulkSender {

  private static byte[] msgBody = new byte[1048576];

  public static void main(String[] args) throws IOException, InterruptedException {
    execute(Argument.parse(new Argument(), args));
  }

  public static void execute(final Argument param) throws IOException, InterruptedException {
    long begin = System.currentTimeMillis();
    System.out.println("create topic begin");
    // you must create topics for best configs
    try (var admin = Admin.create(Map.of(AdminConfigs.BOOTSTRAP_SERVERS_CONFIG, param.bootstrapServers()))) {
      List<NewTopic> topics = new ArrayList<>();
      for (var t : param.topics) {
        topics.add(new NewTopic(t, 8, (short) 1));
      }
      admin.createTopics(topics).all();
    }

    System.out.println("create topic end");
    System.out.println("create topic cost " + (System.currentTimeMillis() - begin));

    // you must manage producers for best performance
    try (var producer =
        new KafkaProducer<>(
            Map.of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, param.bootstrapServers(),
                    ProducerConfig.PARTITIONER_IGNORE_KEYS_CONFIG,
                    "true",
                    ProducerConfig.ACKS_CONFIG,
                    "1",
                    ProducerConfig.COMPRESSION_TYPE_CONFIG,
                    "zstd",
                    ProducerConfig.COMPRESSION_ZSTD_LEVEL_CONFIG,
                    "-100",
                    ProducerConfig.BUFFER_MEMORY_CONFIG,
                    "16777216",
                    ProducerConfig.BATCH_SIZE_CONFIG,
                    "524288",
                    ProducerConfig.LINGER_MS_CONFIG,
                    "2000",
                    ProducerConfig.MAX_REQUEST_SIZE_CONFIG,
                    "1148576"
            ),
            new ByteArraySerializer(),
            new ByteArraySerializer())) {
      System.out.println("created producer");
      var size = new AtomicLong(0);
      var key = "key";
      var value = "value";
      var num = 0;

      System.out.println("param.dataSize.bytes() is " + param.dataSize.bytes());
//      long totalSize = 20 * 1024 * 1024 * 1024L;
      long totalSize = param.dataSize.bytes();
      while (size.get() < totalSize) {
        var topic = param.topics.get(num++ % param.topics.size());
        producer.send(
            new ProducerRecord<>(topic, msgBody),
            (m, e) -> {
              if (e == null) size.addAndGet(m.serializedValueSize());
            });
      }

      producer.flush();
      producer.close();
      System.out.println("total cost " + (System.currentTimeMillis() - begin));
    }
  }

  public static class Argument extends org.astraea.app.argument.Argument {
    @Parameter(
        names = {"--topics"},
        description = "List<String>: topic names which you should send",
        validateWith = StringListField.class,
        listConverter = StringListField.class,
        required = true)
    List<String> topics;

    @Parameter(
        names = {"--dataSize"},
        description = "data size: total size you have to send",
        validateWith = DataSizeField.class,
        converter = DataSizeField.class)
    DataSize dataSize = DataSize.GB.of(10);
  }
}
