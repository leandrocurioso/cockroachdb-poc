-- Optional: pre-split and scatter the bench tables across the cluster
-- before load, instead of waiting for CockroachDB's load-based splitter to
-- react to traffic. Reactive splitting happens *during* the benchmark and
-- each split briefly stalls the range it's splitting — pre-splitting empty
-- tables avoids that churn so throughput ramps up cleanly from the start.
--
-- Applied with: make presplit (run once, right after `make schema`,
-- before `make bench`)

USE bench;

ALTER TABLE client SPLIT AT SELECT gen_random_uuid() FROM generate_series(1, 20);
ALTER TABLE client_status SPLIT AT SELECT gen_random_uuid() FROM generate_series(1, 20);
ALTER TABLE client_address SPLIT AT SELECT gen_random_uuid() FROM generate_series(1, 20);

ALTER TABLE client SCATTER;
ALTER TABLE client_status SCATTER;
ALTER TABLE client_address SCATTER;
