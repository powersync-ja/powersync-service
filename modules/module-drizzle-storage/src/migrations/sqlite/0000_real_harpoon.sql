CREATE TABLE `bucket_data` (
	`id` text PRIMARY KEY NOT NULL,
	`group_id` integer NOT NULL,
	`bucket_name` text NOT NULL,
	`op_id` bigint NOT NULL,
	`op` text NOT NULL,
	`source_table` text,
	`source_key` blob,
	`table_name` text,
	`row_id` text,
	`checksum` bigint NOT NULL,
	`data` text,
	`target_op` bigint
);
--> statement-breakpoint
CREATE INDEX `bucket_data_bucket_op_index` ON `bucket_data` (`group_id`,`bucket_name`,`op_id`);--> statement-breakpoint
CREATE INDEX `bucket_data_source_index` ON `bucket_data` (`group_id`,`source_table`,`source_key`);--> statement-breakpoint
CREATE TABLE `bucket_parameters` (
	`id` bigint PRIMARY KEY NOT NULL,
	`group_id` integer NOT NULL,
	`source_table` text NOT NULL,
	`source_key` blob NOT NULL,
	`lookup` blob NOT NULL,
	`bucket_parameters` text NOT NULL
);
--> statement-breakpoint
CREATE INDEX `bucket_parameters_lookup_index` ON `bucket_parameters` (`group_id`,`lookup`,`id`);--> statement-breakpoint
CREATE INDEX `bucket_parameters_source_index` ON `bucket_parameters` (`group_id`,`source_table`,`source_key`);--> statement-breakpoint
CREATE TABLE `current_data` (
	`id` text PRIMARY KEY NOT NULL,
	`group_id` integer NOT NULL,
	`source_table` text NOT NULL,
	`source_key` blob NOT NULL,
	`buckets` text NOT NULL,
	`lookups` text NOT NULL,
	`data` blob NOT NULL,
	`pending_delete` bigint
);
--> statement-breakpoint
CREATE INDEX `current_data_source_index` ON `current_data` (`group_id`,`source_table`,`source_key`);--> statement-breakpoint
CREATE INDEX `current_data_pending_delete_index` ON `current_data` (`group_id`,`pending_delete`);--> statement-breakpoint
CREATE TABLE `instance` (
	`id` text PRIMARY KEY NOT NULL
);
--> statement-breakpoint
CREATE TABLE `source_tables` (
	`id` text PRIMARY KEY NOT NULL,
	`group_id` integer NOT NULL,
	`connection_id` integer NOT NULL,
	`relation_id` text,
	`schema_name` text NOT NULL,
	`table_name` text NOT NULL,
	`replica_id_columns` text,
	`snapshot_done` integer DEFAULT true NOT NULL,
	`snapshot_total_estimated_count` bigint,
	`snapshot_replicated_count` bigint,
	`snapshot_last_key` blob
);
--> statement-breakpoint
CREATE INDEX `source_table_lookup` ON `source_tables` (`group_id`,`table_name`);--> statement-breakpoint
CREATE TABLE `sync_rules` (
	`id` integer PRIMARY KEY AUTOINCREMENT NOT NULL,
	`state` text NOT NULL,
	`snapshot_done` integer DEFAULT false NOT NULL,
	`snapshot_lsn` text,
	`last_checkpoint` bigint,
	`last_checkpoint_lsn` text,
	`no_checkpoint_before` text,
	`slot_name` text NOT NULL,
	`last_checkpoint_ts` integer,
	`last_keepalive_ts` integer,
	`last_fatal_error` text,
	`last_fatal_error_ts` integer,
	`keepalive_op` bigint,
	`storage_version` integer,
	`content` text NOT NULL,
	`sync_plan` text
);
--> statement-breakpoint
CREATE TABLE `write_checkpoints` (
	`id` text PRIMARY KEY NOT NULL,
	`sync_rules_id` integer,
	`user_id` text NOT NULL,
	`checkpoint` bigint NOT NULL,
	`heads` text,
	`created_at` integer NOT NULL
);
--> statement-breakpoint
CREATE INDEX `write_checkpoints_user_checkpoint_index` ON `write_checkpoints` (`user_id`,`sync_rules_id`,`checkpoint`);