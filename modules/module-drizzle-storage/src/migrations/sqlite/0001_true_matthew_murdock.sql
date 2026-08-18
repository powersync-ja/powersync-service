CREATE TABLE `op_id_sequence` (
	`id` integer PRIMARY KEY NOT NULL,
	`next_op_id` bigint NOT NULL
);
--> statement-breakpoint
INSERT INTO `op_id_sequence` (`id`, `next_op_id`)
SELECT 1, MAX(`max_op_id`) + 1
FROM (
	SELECT COALESCE(MAX(`op_id`), 0) AS `max_op_id` FROM `bucket_data`
	UNION ALL
	SELECT COALESCE(MAX(`id`), 0) AS `max_op_id` FROM `bucket_parameters`
	UNION ALL
	SELECT COALESCE(MAX(`pending_delete`), 0) AS `max_op_id` FROM `current_data`
);
