import { SyncConfig } from '../SyncConfig.js';
import { SourceSchema } from '../types.js';
import { SchemaGenerator, toCamelCase } from './SchemaGenerator.js';

/**
 * Generates a schema to use with the [Room library](https://docs.powersync.com/client-sdk-references/kotlin-multiplatform/libraries/room).
 */
export class RoomSchemaGenerator extends SchemaGenerator {
  readonly key = 'kotlin-room';
  readonly label = 'Room (Kotlin Multiplatform)';
  readonly mediaType = 'text/x-kotlin';
  readonly fileName = 'Entities.kt';

  generate(source: SyncConfig, schema: SourceSchema): string {
    let buffer = `import androidx.room.ColumnInfo
import androidx.room.Entity
import androidx.room.PrimaryKey
`;
    const tables = super.getAllTables(source, schema);
    for (const table of tables) {
      // @Entity(tableName = "todo_list_items") data class TodoListItems(
      buffer += `\n@Entity(tableName = "${table.name}")\n`;
      buffer += `data class ${toCamelCase(table.name, true)}(\n`;

      // Id column
      buffer += '  @PrimaryKey val id: String,\n';

      for (const column of table.columns) {
        const sqliteType = this.columnType(column);
        const kotlinType = {
          text: 'String',
          real: 'Double',
          integer: 'Long'
        }[sqliteType];

        // @ColumnInfo(name = "author_id") val authorId: String,
        buffer += `  @ColumnInfo("${column.name}") val ${toCamelCase(column.name)}: ${kotlinType},\n`;
      }

      buffer += ')\n';
    }

    return buffer;
  }
}
