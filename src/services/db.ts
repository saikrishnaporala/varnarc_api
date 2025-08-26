import mysql, { Pool } from 'mysql2/promise';

let pool: Pool | null = null;

export async function getDatabasePool(): Promise<Pool> {
  if (!pool) {
    const requiredBase = ['DB_USER', 'DB_DATABASE'];
    for (const key of requiredBase) {
      if (!process.env[key]) {
        throw new Error(`Missing env var ${key}`);
      }
    }

    const useSocket = Boolean(process.env.DB_SOCKET_PATH);
    const ssl = /^true$/i.test(process.env.DB_SSL || '');

    const common = {
      user: process.env.DB_USER as string,
      password: (process.env.DB_PASSWORD ?? '') as string,
      database: process.env.DB_DATABASE as string,
      waitForConnections: true,
      connectionLimit: 10,
      queueLimit: 0,
      decimalNumbers: true as const,
      ssl: ssl ? { rejectUnauthorized: false } : undefined
    };

    pool = useSocket
      ? mysql.createPool({
          ...common,
          socketPath: process.env.DB_SOCKET_PATH as string
        })
      : mysql.createPool({
          ...common,
          host: (process.env.DB_HOST || 'localhost') as string,
          port: Number(process.env.DB_PORT || 3306)
        });
  }
  return pool;
}

export async function ensureDatabaseConnection(): Promise<void> {
  const p = await getDatabasePool();
  const conn = await p.getConnection();
  await conn.ping();
  conn.release();
}


