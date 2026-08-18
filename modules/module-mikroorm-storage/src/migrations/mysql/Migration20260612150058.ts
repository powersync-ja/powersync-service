import { Migration } from '@mikro-orm/migrations';

export class Migration20260612150058 extends Migration {

  override up(): void | Promise<void> {
    this.addSql(`create table \`bucket_data\` (\`id\` varchar(255) not null, \`group_id\` int not null, \`bucket_name\` varchar(255) not null, \`op_id\` bigint not null, \`op\` varchar(255) not null, \`source_table\` varchar(255) null, \`source_key\` varbinary(1024) null, \`table_name\` varchar(255) null, \`row_id\` varchar(255) null, \`checksum\` bigint not null, \`data\` longtext null, \`target_op\` bigint null, primary key (\`id\`)) default character set utf8mb4 engine = InnoDB;`);
    this.addSql(`alter table \`bucket_data\` add index \`bucket_data_bucket_op_index\` (\`group_id\`, \`bucket_name\`, \`op_id\`);`);
    this.addSql(`alter table \`bucket_data\` add index \`bucket_data_source_index\` (\`group_id\`, \`source_table\`, \`source_key\`);`);

    this.addSql(`create table \`bucket_parameters\` (\`id\` bigint unsigned not null, \`group_id\` int not null, \`source_table\` varchar(255) not null, \`source_key\` varbinary(1024) not null, \`lookup\` varbinary(1024) not null, \`bucket_parameters\` json not null, primary key (\`id\`)) default character set utf8mb4 engine = InnoDB;`);
    this.addSql(`alter table \`bucket_parameters\` add index \`bucket_parameters_lookup_index\` (\`group_id\`, \`lookup\`, \`id\`);`);
    this.addSql(`alter table \`bucket_parameters\` add index \`bucket_parameters_source_index\` (\`group_id\`, \`source_table\`, \`source_key\`);`);

    this.addSql(`create table \`current_data\` (\`id\` varchar(255) not null, \`group_id\` int not null, \`source_table\` varchar(255) not null, \`source_key\` varbinary(1024) not null, \`buckets\` json not null, \`lookups\` json not null, \`data\` longblob not null, \`pending_delete\` bigint null, primary key (\`id\`)) default character set utf8mb4 engine = InnoDB;`);
    this.addSql(`alter table \`current_data\` add index \`current_data_source_index\` (\`group_id\`, \`source_table\`, \`source_key\`);`);
    this.addSql(`alter table \`current_data\` add index \`current_data_pending_delete_index\` (\`group_id\`, \`pending_delete\`);`);

    this.addSql(`create table \`instance\` (\`id\` varchar(255) not null, primary key (\`id\`)) default character set utf8mb4 engine = InnoDB;`);

    this.addSql(`create table \`source_tables\` (\`id\` varchar(255) not null, \`group_id\` int not null, \`connection_id\` int not null, \`relation_id\` json null, \`schema_name\` varchar(255) not null, \`table_name\` varchar(255) not null, \`replica_id_columns\` json null, \`snapshot_done\` tinyint(1) not null default true, \`snapshot_total_estimated_count\` bigint null, \`snapshot_replicated_count\` bigint null, \`snapshot_last_key\` blob null, primary key (\`id\`)) default character set utf8mb4 engine = InnoDB;`);
    this.addSql(`alter table \`source_tables\` add index \`source_table_lookup\` (\`group_id\`, \`table_name\`);`);

    this.addSql(`create table \`sync_rules\` (\`id\` int unsigned not null auto_increment primary key, \`state\` varchar(255) not null, \`snapshot_done\` tinyint(1) not null default false, \`snapshot_lsn\` varchar(255) null, \`last_checkpoint\` bigint null, \`last_checkpoint_lsn\` varchar(255) null, \`no_checkpoint_before\` varchar(255) null, \`slot_name\` varchar(255) not null, \`last_checkpoint_ts\` datetime null, \`last_keepalive_ts\` datetime null, \`last_fatal_error\` varchar(255) null, \`last_fatal_error_ts\` datetime null, \`keepalive_op\` bigint null, \`storage_version\` int null, \`content\` longtext not null, \`sync_plan\` json null) default character set utf8mb4 engine = InnoDB;`);

    this.addSql(`create table \`write_checkpoints\` (\`id\` varchar(255) not null, \`sync_rules_id\` int null, \`user_id\` varchar(255) not null, \`checkpoint\` bigint not null, \`heads\` json null, \`created_at\` datetime not null, primary key (\`id\`)) default character set utf8mb4 engine = InnoDB;`);
    this.addSql(`alter table \`write_checkpoints\` add index \`write_checkpoints_user_checkpoint_index\` (\`user_id\`, \`sync_rules_id\`, \`checkpoint\`);`);
  }

}
