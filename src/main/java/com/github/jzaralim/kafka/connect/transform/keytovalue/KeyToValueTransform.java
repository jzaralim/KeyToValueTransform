/**
 * Copyright © 2019 Zara Lim (jzaralim@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.jzaralim.kafka.connect.transform.keytovalue;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public class KeyToValueTransform<R extends ConnectRecord<R>> implements Transformation<R> {

  public static final String OVERVIEW_DOC = "Add the record key to the value as a field.";

  public static final ConfigDef CONFIG_DEF = new ConfigDef();

  private static final String PURPOSE = "copying fields from value to key";

  private Cache<Schema, Schema> valueToKeySchemaCache;

  private Cache<Schema, Schema> updatedSchemaCache;

  public void configure(Map<String, ?> configs) {
    final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
    valueToKeySchemaCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
    updatedSchemaCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
  }

  public R apply(R record)  {
    if (record.valueSchema() == null) {
      return applySchemaless(record);
    } else {
      return applyWithSchema(record);
    }
  }

  private R applySchemaless(R record) {
    final Map<String, Object> value;
    if (record.value() == null) {
      value = new HashMap<String, Object>(1);
    } else {
      value = requireMap(record.value(), PURPOSE);
    }
    final Map<String, Object> key = new HashMap<String, Object>(1);
    key.put("rowkey", record.key());
    value.put("rowkey", record.key());
    return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        null,
        key,
        record.valueSchema(),
        record.value(),
        record.timestamp()
    );
  }

  private R applyWithSchema(R record) {
    final Struct value = requireStruct(record.value(), PURPOSE);

    Schema updatedSchema = updatedSchemaCache.get(value.schema());
    Schema keySchema = valueToKeySchemaCache.get(value.schema());

    if (updatedSchema == null) {
      final SchemaBuilder valueSchemaBuilder = SchemaBuilder.struct();
      for (Field field : value.schema().fields()) {
        valueSchemaBuilder.field(field.name(), field.schema());
      }
      valueSchemaBuilder.field("rowkey", record.keySchema());
      updatedSchema = valueSchemaBuilder.build();

      updatedSchemaCache.put(value.schema(), updatedSchema);
    }

    if (keySchema == null) {
      final SchemaBuilder keySchemaBuilder = SchemaBuilder.struct();
      keySchemaBuilder.field("rowkey", record.keySchema());
      keySchema = keySchemaBuilder.build();
      valueToKeySchemaCache.put(value.schema(), keySchema);
    }

    final Struct key = new Struct(keySchema);
    final Struct newValue = new Struct(updatedSchema);
    for (Field field : value.schema().fields()) {
      newValue.put(field.name(), value.get(field));
    }

    key.put("rowkey", record.key());
    newValue.put("rowkey", record.key());

    return record.newRecord(record.topic(), record.kafkaPartition(), keySchema, key, newValue.schema(), newValue, record.timestamp());
  }

  public ConfigDef config() {
    return CONFIG_DEF;
  }

  public void close() {
    valueToKeySchemaCache = null;
  }

}