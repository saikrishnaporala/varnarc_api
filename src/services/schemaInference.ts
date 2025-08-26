export type SqlType =
  | 'INT'
  | 'BIGINT'
  | 'DOUBLE'
  | 'DECIMAL'
  | 'BOOLEAN'
  | 'DATE'
  | 'DATETIME'
  | 'VARCHAR'
  | 'TEXT'
  | 'LONGTEXT';

export interface ColumnSchema {
  name: string; // sanitized SQL identifier
  originalHeader: string; // original file header to map values
  type: SqlType;
  length?: number; // for VARCHAR
  nullable: boolean;
}

export interface TableSchema {
  columns: ColumnSchema[];
}

export function sanitizeSqlIdentifier(name: string): string {
  let sanitized = name.trim().toLowerCase().replace(/[^a-z0-9_]+/g, '_');
  if (!sanitized || /^[0-9]/.test(sanitized)) {
    sanitized = `col_${sanitized}`;
  }
  sanitized = sanitized.replace(/^_+|_+$/g, '').replace(/__+/g, '_');
  return sanitized || 'col_unnamed';
}

function isIntegerLike(value: any): boolean {
  if (value === null || value === undefined || value === '') return false;
  if (typeof value === 'number') return Number.isInteger(value);
  const s = String(value).trim();
  if (!/^[-+]?\d+$/.test(s)) return false;
  const n = Number(s);
  return Number.isInteger(n);
}

function isBigIntRange(value: any): boolean {
  const n = typeof value === 'number' ? value : Number(String(value));
  return n > 2147483647 || n < -2147483648; // beyond 32-bit
}

function isFloatLike(value: any): boolean {
  if (value === null || value === undefined || value === '') return false;
  if (typeof value === 'number') return !Number.isNaN(value) && !Number.isInteger(value);
  const s = String(value).trim();
  if (!/^[-+]?\d*(?:\.\d+)?$/.test(s)) return false;
  return s.includes('.') || !isIntegerLike(value);
}

function isBooleanLike(value: any): boolean {
  if (typeof value === 'boolean') return true;
  const s = String(value).trim().toLowerCase();
  return ['true', 'false', 'yes', 'no', 'y', 'n', '1', '0'].includes(s);
}

function isIsoDate(value: any): boolean {
  if (value == null || value === '') return false;
  const s = String(value).trim();
  // YYYY-MM-DD
  return /^\d{4}-\d{2}-\d{2}$/.test(s);
}

function isIsoDateTime(value: any): boolean {
  if (value == null || value === '') return false;
  const s = String(value).trim();
  // YYYY-MM-DD HH:MM:SS or with T
  return /^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?$/.test(s);
}

function measureMaxLength(values: any[]): number {
  let max = 0;
  for (const v of values) {
    if (v == null) continue;
    const len = String(v).length;
    if (len > max) max = len;
  }
  return max;
}

export function inferTableSchema(headers: string[], rows: Record<string, any>[]): TableSchema {
  // Ensure unique sanitized names
  const used = new Set<string>();
  const columns: ColumnSchema[] = headers.map((headerRaw) => {
    const header = headerRaw ?? 'column';
    let name = sanitizeSqlIdentifier(header);
    let suffix = 1;
    while (used.has(name)) {
      name = `${name}_${suffix++}`;
    }
    used.add(name);
    return {
      name,
      originalHeader: header,
      type: 'LONGTEXT',  // 👈 default to LONGTEXT for all
      nullable: true
    } as ColumnSchema;
  });

  // sample rows (up to 1000)
  const sample = rows.slice(0, 1000);

  for (const col of columns) {
    const values = sample.map(r => r[col.originalHeader]).filter(v => v !== undefined);
    const nonNull = values.filter(v => v !== null && v !== '');
    const nullable = nonNull.length < values.length;

    let chosen: SqlType = 'LONGTEXT'; // 👈 force default LONGTEXT

    if (nonNull.length === 0) {
      chosen = 'LONGTEXT';
    } else if (nonNull.every(isBooleanLike)) {
      chosen = 'BOOLEAN';
    } else if (nonNull.every(isIntegerLike)) {
      chosen = nonNull.some(isBigIntRange) ? 'BIGINT' : 'BIGINT';
    } else if (nonNull.every(v => isIntegerLike(v) || isFloatLike(v))) {
      chosen = 'DOUBLE';
    } else if (nonNull.every(isIsoDateTime)) {
      chosen = 'DATETIME';
    } else if (nonNull.every(v => isIsoDateTime(v) || isIsoDate(v))) {
      chosen = 'DATETIME';
    } else if (nonNull.every(isIsoDate)) {
      chosen = 'DATE';
    } else {
      chosen = 'LONGTEXT'; // 👈 force LONGTEXT for all other cases
    }

    col.type = chosen;
    col.nullable = nullable;
    delete col.length; // 👈 remove VARCHAR length logic completely
  }

  return { columns };
}


export function generateCreateTableSql(
  tableName: string,
  schema: TableSchema,
  _ifExists: string,
  nullability: 'all-nullable' | 'inferred' = 'all-nullable'
): string {
  const cols = schema.columns.map(c => {
    const typeStr = c.type === 'VARCHAR' && c.length ? `VARCHAR(${c.length})` : c.type;
    const allowNull = nullability === 'all-nullable' ? true : c.nullable;
    const nullStr = allowNull ? 'NULL' : 'NOT NULL';
    return `\`${c.name}\` ${typeStr} ${nullStr}`;
  });

  const create = `CREATE TABLE IF NOT EXISTS \`${tableName}\` (\n${cols.join(',\n')}\n)`;
  return create;
}

export function mapRowToSqlColumns(
  row: Record<string, any>,
  headers: string[],
  schema: TableSchema
): any[] {
  const values: any[] = [];
  for (const col of schema.columns) {
    const raw = row[col.originalHeader];
    if (raw == null || raw === '') {
      values.push(null);
      continue;
    }
    switch (col.type) {
      case 'BOOLEAN': {
        const s = String(raw).trim().toLowerCase();
        values.push(['true', 'yes', 'y', '1'].includes(s) ? 1 : 0);
        break;
      }
      case 'INT':
      case 'BIGINT': {
        const n = Number.parseInt(String(raw), 10);
        values.push(Number.isNaN(n) ? null : n);
        break;
      }
      case 'DOUBLE':
      case 'DECIMAL': {
        const n = Number(String(raw));
        values.push(Number.isNaN(n) ? null : n);
        break;
      }
      case 'DATE':
      case 'DATETIME':
        values.push(String(raw));
        break;
      case 'TEXT':
      case 'LONGTEXT':
      case 'VARCHAR':
      default:
        values.push(String(raw));
        break;
    }
  }
  return values;
}



