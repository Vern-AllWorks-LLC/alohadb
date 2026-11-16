# Copyright (c) 2025, AlohaDB

rdkafka_dep = dependency('rdkafka', required: false)

alohadb_kafka_sources = files(
  'alohadb_kafka.c',
  'kafka_consumer.c',
)

if host_system == 'windows'
  alohadb_kafka_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'alohadb_kafka',
    '--FILEDESC', 'alohadb_kafka - inbound Kafka and Kinesis streaming consumer',])
endif

kafka_mod_args = contrib_mod_args
if rdkafka_dep.found()
  kafka_mod_args += {'dependencies': [rdkafka_dep]}
  add_project_arguments('-DUSE_RDKAFKA', language: 'c')
endif

alohadb_kafka = shared_module('alohadb_kafka',
  alohadb_kafka_sources,
  c_pch: pch_postgres_h,
  kwargs: kafka_mod_args,
)
contrib_targets += alohadb_kafka

install_data(
  'alohadb_kafka.control',
  'alohadb_kafka--1.0.sql',
  kwargs: contrib_data_args,
)
