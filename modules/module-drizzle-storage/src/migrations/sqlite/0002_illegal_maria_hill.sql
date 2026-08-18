ALTER TABLE `write_checkpoints` ADD `checkpoint_requested_at` integer;--> statement-breakpoint
CREATE INDEX `write_checkpoints_requested_at_index` ON `write_checkpoints` (`checkpoint_requested_at`);