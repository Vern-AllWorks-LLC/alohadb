# Copyright (c) 2025, AlohaDB Project

test_simulation = shared_module('test_simulation',
  files(
    'test_simulation.c',
    'sim_prng.c',
    'sim_fault.c',
  ),
  kwargs: pg_test_mod_args,
)
test_install_libs += test_simulation

install_data(
  'test_simulation.control',
  'test_simulation--1.0.sql',
  kwargs: contrib_data_args,
)
