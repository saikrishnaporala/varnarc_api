import { getDatabasePool } from "../services/db";

// Helpers to support row-level actions
async function ensureIdColumn(table: string): Promise<void> {
    const pool = await getDatabasePool();
    const conn = await pool.getConnection();
    try {
        const [desc] = await conn.query<any[]>(`DESCRIBE \`${table}\``);
        const columns: any[] = Array.isArray(desc) ? (desc as any[]) : [];
        const hasId = columns.some(c => c.Field === '_id');
        const hasPrimary = columns.some(c => (c.Key || '').toUpperCase() === 'PRI');
        if (!hasId) {
            if (hasPrimary) {
                await conn.query(`ALTER TABLE \`${table}\` ADD COLUMN \`_id\` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT, ADD UNIQUE KEY \`_id_unique\` (\`_id\`)`);
            } else {
                await conn.query(`ALTER TABLE \`${table}\` ADD COLUMN \`_id\` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY`);
            }
        }
    } finally {
        conn.release();
    }
}

async function ensureActiveColumn(table: string): Promise<void> {
    const pool = await getDatabasePool();
    const conn = await pool.getConnection();
    try {
        const [desc] = await conn.query<any[]>(`DESCRIBE \`${table}\``);
        const columns: any[] = Array.isArray(desc) ? (desc as any[]) : [];
        const hasActive = columns.some(c => c.Field === 'active');
        if (!hasActive) {
            await conn.query(`ALTER TABLE \`${table}\` ADD COLUMN \`active\` TINYINT(1) NOT NULL DEFAULT 1`);
        }
    } finally {
        conn.release();
    }
}