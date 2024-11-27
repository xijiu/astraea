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
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.astraea.app.argument.DataSizeField;
import org.astraea.app.argument.StringListField;
import org.astraea.common.DataSize;
import org.astraea.common.admin.AdminConfigs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class BulkSender2 {

  private static byte[] msgBody = new byte[1018576];

  public static void main(String[] args) throws IOException, InterruptedException {
    Argument argument = new Argument();
    argument.topics = new ArrayList<>();
    argument.topics.add("topic1");
//    argument.topics.add("topic2");
    argument.dataSize = DataSize.of("300MB");
    execute(argument);
  }

  public static void execute(final Argument param) throws IOException, InterruptedException {
    // you must create topics for best configs
//    try (var admin =
//        Admin.create(Map.of(AdminConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"))) {
//      for (var t : param.topics) {
//        admin.createTopics(List.of(new NewTopic(t, 8, (short) 1))).all();
//      }
//    }
    // you must manage producers for best performance
    try (var producer =
        new KafkaProducer<>(
            Map.of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    "localhost:9092",
                    ProducerConfig.PARTITIONER_IGNORE_KEYS_CONFIG,
                    "true",
                    ProducerConfig.ACKS_CONFIG,
                    "0",
                    ProducerConfig.COMPRESSION_TYPE_CONFIG,
                    "zstd",
                    ProducerConfig.COMPRESSION_ZSTD_LEVEL_CONFIG,
                    "-100",
                    ProducerConfig.BUFFER_MEMORY_CONFIG,
                    "16777216",
                    ProducerConfig.BATCH_SIZE_CONFIG,
                    "524288",
                    ProducerConfig.LINGER_MS_CONFIG,
                    "1",
                    ProducerConfig.MAX_REQUEST_SIZE_CONFIG,
                    "1048576"
            ),
            new ByteArraySerializer(),
            new ByteArraySerializer())) {
      var size = new AtomicLong(0);
      var num = 0;
      while (size.get() < param.dataSize.bytes()) {
        var topic = param.topics.get(num++ % param.topics.size());
        producer.send(
            new ProducerRecord<>(topic, msgBody),
            (m, e) -> {
              if (e == null) {
                size.addAndGet(m.serializedValueSize());
                System.out.println("::::::hahah: " + size);
              }
            });
      }
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
