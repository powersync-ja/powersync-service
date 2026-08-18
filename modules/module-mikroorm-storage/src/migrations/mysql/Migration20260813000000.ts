import { Migration } from '@mikro-orm/migrations';

export class Migration20260813000000 extends Migration {
  override up(): void | Promise<void> {
    this.addSql(`alter table \`write_checkpoints\` add \`checkpoint_requested_at\` datetime null;`);
    this.addSql(
      `alter table \`write_checkpoints\` add index \`write_checkpoints_requested_at_index\` (\`checkpoint_requested_at\`);`
    );
  }
}
