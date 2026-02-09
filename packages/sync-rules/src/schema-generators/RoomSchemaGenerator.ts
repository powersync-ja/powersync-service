import { BaseSyncConfig } from '../BaseSyncConfig.js';
import { SourceSchema } from '../types.js';
import { SchemaGenerator } from './SchemaGenerator.js';

/**
 * Generates a schema to use with the [Room library](https://docs.powersync.com/client-sdk-references/kotlin-multiplatform/libraries/room).
 */
export class RoomSchemaGenerator extends SchemaGenerator {
  readonly key = 'kotlin-room';
  readonly label = 'Room (Kotlin Multiplatform)';
  readonly mediaType = 'text/x-kotlin';
  readonly fileName = 'Entities.kt';

  generate(source: BaseSyncConfig, schema: SourceSchema): string {
    let buffer = `import androidx.room.ColumnInfo
import androidx.room.Entity
import androidx.room.PrimaryKey
`;
    const tables = super.getAllTables(source, schema);
    for (const table of tables) {
      // @Entity(tableName = "todo_list_items") data class TodoListItems(
      buffer += `\n@Entity(tableName = "${table.name}")\n`;
      buffer += `data class ${snakeCaseToKotlin(table.name, true)}(\n`;

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
        buffer += `  @ColumnInfo("${column.name}") val ${snakeCaseToKotlin(column.name, false)}: ${kotlinType},\n`;
      }

      buffer += ')\n';
    }

    return buffer;
  }
}

function snakeCaseToKotlin(source: string, initialUpper: boolean): string {
  let result = '';
  for (const chunk of source.split('_')) {
    if (chunk.length == 0) continue;

    const firstCharUpper = result.length > 0 || initialUpper;
    if (firstCharUpper) {
      result += chunk.charAt(0).toUpperCase();
      result += chunk.substring(1);
    } else {
      result += chunk;
    }
  }

  return result;
}
