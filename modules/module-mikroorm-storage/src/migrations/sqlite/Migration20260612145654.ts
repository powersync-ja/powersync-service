import { Migration } from '@mikro-orm/migrations';

export class Migration20260612145654 extends Migration {

  override up(): void | Promise<void> {
    this.addSql(`create table \`bucket_data\` (\`id\` text not null primary key, \`group_id\` integer not null, \`bucket_name\` text not null, \`op_id\` bigint not null, \`op\` text not null, \`source_table\` text null, \`source_key\` blob null, \`table_name\` text null, \`row_id\` text null, \`checksum\` bigint not null, \`data\` text null, \`target_op\` bigint null);`);
    this.addSql(`create index \`bucket_data_bucket_op_index\` on \`bucket_data\` (\`group_id\`, \`bucket_name\`, \`op_id\`);`);
    this.addSql(`create index \`bucket_data_source_index\` on \`bucket_data\` (\`group_id\`, \`source_table\`, \`source_key\`);`);

    this.addSql(`create table \`bucket_parameters\` (\`id\` bigint not null primary key, \`group_id\` integer not null, \`source_table\` text not null, \`source_key\` blob not null, \`lookup\` blob not null, \`bucket_parameters\` json not null);`);
    this.addSql(`create index \`bucket_parameters_lookup_index\` on \`bucket_parameters\` (\`group_id\`, \`lookup\`, \`id\`);`);
    this.addSql(`create index \`bucket_parameters_source_index\` on \`bucket_parameters\` (\`group_id\`, \`source_table\`, \`source_key\`);`);

    this.addSql(`create table \`current_data\` (\`id\` text not null primary key, \`group_id\` integer not null, \`source_table\` text not null, \`source_key\` blob not null, \`buckets\` json not null, \`lookups\` json not null, \`data\` blob not null, \`pending_delete\` bigint null);`);
    this.addSql(`create index \`current_data_source_index\` on \`current_data\` (\`group_id\`, \`source_table\`, \`source_key\`);`);
    this.addSql(`create index \`current_data_pending_delete_index\` on \`current_data\` (\`group_id\`, \`pending_delete\`);`);

    this.addSql(`create table \`instance\` (\`id\` text not null primary key);`);

    this.addSql(`create table \`source_tables\` (\`id\` text not null primary key, \`group_id\` integer not null, \`connection_id\` integer not null, \`relation_id\` json null, \`schema_name\` text not null, \`table_name\` text not null, \`replica_id_columns\` json null, \`snapshot_done\` integer not null default true, \`snapshot_total_estimated_count\` bigint null, \`snapshot_replicated_count\` bigint null, \`snapshot_last_key\` blob null);`);
    this.addSql(`create index \`source_table_lookup\` on \`source_tables\` (\`group_id\`, \`table_name\`);`);

    this.addSql(`create table \`sync_rules\` (\`id\` integer not null primary key autoincrement, \`state\` text not null, \`snapshot_done\` integer not null default false, \`snapshot_lsn\` text null, \`last_checkpoint\` bigint null, \`last_checkpoint_lsn\` text null, \`no_checkpoint_before\` text null, \`slot_name\` text not null, \`last_checkpoint_ts\` datetime null, \`last_keepalive_ts\` datetime null, \`last_fatal_error\` text null, \`last_fatal_error_ts\` datetime null, \`keepalive_op\` bigint null, \`storage_version\` integer null, \`content\` text not null, \`sync_plan\` json null);`);

    this.addSql(`create table \`write_checkpoints\` (\`id\` text not null primary key, \`sync_rules_id\` integer null, \`user_id\` text not null, \`checkpoint\` bigint not null, \`heads\` json null, \`created_at\` datetime not null);`);
    this.addSql(`create index \`write_checkpoints_user_checkpoint_index\` on \`write_checkpoints\` (\`user_id\`, \`sync_rules_id\`, \`checkpoint\`);`);
  }

}
