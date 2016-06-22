#!/bin/sh

rm -rf core data/ meta_state.dump* zoolog.log 
GTEST_FILTER="meta.state_sync:meta.update_configuration:meta.balancer_validator" ./dsn.meta.test
rm -rf core data/ meta_state.dump* zoolog.log
GTEST_FILTER="meta.data_definition" ./dsn.meta.test
rm -rf core data/ meta_state.dump* zoolog.log
GTEST_FILTER="meta.apply_balancer" ./dsn.meta.test
