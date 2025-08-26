import express, { Request, Response } from 'express';
import cors from 'cors';
import multer from 'multer';
import path from 'path';
import fs from 'fs';
import dotenv from 'dotenv';
import { getDatabasePool, ensureDatabaseConnection } from './services/db';
import { parseCsvFile, parseExcelFile, ParsedData } from './services/parsers';
import { inferTableSchema, generateCreateTableSql, sanitizeSqlIdentifier, mapRowToSqlColumns } from './services/schemaInference';
import { downloadToTemp } from './services/downloader';
import { listFilesInFolder, listFilesRecursively } from './services/googleDrive';
import { registerFileUrls, registerUploadedFile } from './services/fileImporter';
import fileRoutes from "./services/files";

dotenv.config();

const app = express();

app.use(cors());
app.use(express.json());

app.use(express.json({ limit: '10mb' }));

app.use("/api/files", fileRoutes);

app.use('/admin', express.static(path.join(process.cwd(), 'public', 'admin')));

const uploadDir = path.join(process.cwd(), 'uploads');
if (!fs.existsSync(uploadDir)) {
  fs.mkdirSync(uploadDir, { recursive: true });
}

const storage = multer.diskStorage({
  destination: (_req, _file, cb) => cb(null, uploadDir),
  filename: (_req, file, cb) => {
    const uniqueSuffix = `${Date.now()}-${Math.round(Math.random() * 1e9)}`;
    const ext = path.extname(file.originalname) || '';
    cb(null, `${file.fieldname}-${uniqueSuffix}${ext}`);
  }
});

const upload = multer({ storage });

app.get('/health', (_req, res) => {
  res.json({ ok: true });
});

app.get('/', (_req, res) => {
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.end(`<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Data Importer</title>
    <style>
      body{font-family:system-ui,-apple-system,Segoe UI,Roboto;max-width:720px;margin:40px auto;padding:0 16px}
      header{display:flex;justify-content:space-between;align-items:center}
      .card{border:1px solid #ddd;border-radius:8px;padding:16px;margin-top:16px}
      input[type=file]{margin:12px 0}
      button{background:#0d6efd;color:#fff;border:none;border-radius:6px;padding:10px 14px;cursor:pointer}
      button:hover{background:#0b5ed7}
      code{background:#f6f8fa;padding:2px 6px;border-radius:4px}
      .row{margin:6px 0}
    </style>
  </head>
  <body>
    <header>
      <h1>Data Importer</h1>
      <a href="/health">health</a>
    </header>

    <div class="card">
      <h3>Upload CSV / Excel</h3>
      <form action="/upload" method="post" enctype="multipart/form-data">
        <div class="row">
          <label>File: <input name="file" type="file" required /></label>
        </div>
        <div class="row">
          <label>Table name (optional): <input name="tableName" type="text" placeholder="derived from filename" /></label>
        </div>
        <div class="row">
          <label>If exists: 
            <select name="ifExists">
              <option value="append" selected>append</option>
              <option value="replace">replace</option>
              <option value="fail">fail</option>
            </select>
          </label>
        </div>
        <button type="submit">Import</button>
      </form>
      <p style="margin-top:12px">Or use curl:
        <br />
        <code>curl -F "file=@/path/to/data.csv" "http://localhost:${process.env.PORT || 4000}/upload?tableName=my_table&ifExists=append"</code>
      </p>
    </div>
  </body>
 </html>`);
});

app.post('/upload', upload.single('file'), async (req: Request, res: Response) => {
  if (!req.file && !req.query.fileUrl && !req.body.fileUrl) {
    return res.status(400).json({ error: 'Provide a file upload (field "file") or fileUrl query/body param.' });
  }

  const ifExists = (req.query.ifExists as string) || 'append'; // 'append' | 'replace' | 'fail'
  const explicitTableName = (req.query.tableName as string) || '';
  const cleanupPaths: string[] = [];
  const processedResults: any[] = [];
  try {
    await ensureDatabaseConnection();

    // Case 1: file URL (Google Drive folder URL)
    if (req.query.fileUrl || req.body.fileUrl) {
      const url = String(req.query.fileUrl || req.body.fileUrl);
      const match = url.match(/[-\w]{25,}/);
      if (!match) {
        return res.status(400).json({ error: "Invalid Google Drive folder URL" });
      }
      const folderId = match[0];

      // ✅ Only register files in DB
      const files = await registerFileUrls(folderId);

      return res.json({ message: "Files registered successfully", files });
    }

    // Case 2: direct file upload (from form-data `file`)
    if (req.file) {
      const fileRecord = await registerUploadedFile(req.file);

      return res.json({
        message: "Registered uploaded file",
        registered: {
          id: fileRecord.id,
          name: fileRecord.originalName,
          status: fileRecord.status
        }
      });
    }

    return res.status(400).json({ error: "Provide a file upload (field 'file') or fileUrl param." });

  } catch (err: any) {
    console.error(err);
    res.status(500).json({ error: err.message || "Internal server error" });
  }
});

app.post('/upload_old', upload.single('file'), async (req: Request, res: Response) => {
  if (!req.file && !req.query.fileUrl && !req.body.fileUrl) {
    return res.status(400).json({ error: 'Provide a file upload (field "file") or fileUrl query/body param.' });
  }

  const ifExists = (req.query.ifExists as string) || 'append'; // 'append' | 'replace' | 'fail'
  const explicitTableName = (req.query.tableName as string) || '';
  const cleanupPaths: string[] = [];
  const processedResults: any[] = [];

  try {
    await ensureDatabaseConnection();

    let localPath = req.file?.path;
    let originalName = req.file?.originalname;

    if (!localPath) {
      const url = String(req.query.fileUrl || req.body.fileUrl);
      const match = url.match(/[-\w]{25,}/);
      if (!match) {
        return res.status(400).json({ error: "Invalid Google Drive folder URL" });
      }
      const folderId = match[0];

      // Fetch files from Google Drive
      const files = await listFilesRecursively(folderId);
      console.log("Files List count : " + files.length);

      const pool = await getDatabasePool();
      const conn = await pool.getConnection();

      try {
        for (const file of files) {
          const dl = await downloadToTemp(file.webViewLink);
          localPath = dl.filePath;
          originalName = dl.filename;

          let ext = path.extname(originalName || '').toLowerCase();
          const overrideTypeRaw = String((req.query.fileType || req.query.type || req.body?.fileType || '') as string).toLowerCase();
          const overrideMap: Record<string, string> = { csv: '.csv', xlsx: '.xlsx', xls: '.xls' };
          if (!['.csv', '.xlsx', '.xls'].includes(ext)) {
            if (overrideMap[overrideTypeRaw]) {
              ext = overrideMap[overrideTypeRaw];
            }
          }

          // Insert / update file entry
          await conn.query(
            `INSERT INTO files (id, name, mime_type, web_view_link, row_count, status) 
             VALUES (?, ?, ?, ?, ?, ?) 
             ON DUPLICATE KEY UPDATE status='processing'`,
            [file.id, file.name, file.mimeType, file.webViewLink, 0, 'processing']
          );

          let parsed: ParsedData | null = null;
          if (ext === '.csv') {
            parsed = await parseCsvFile(localPath);
          } else if (ext === '.xlsx' || ext === '.xls') {
            parsed = await parseExcelFile(localPath);
          } else {
            await conn.query(`UPDATE files SET status='unsupported' WHERE id=?`, [file.id]);
            continue;
          }

          if (!parsed || parsed.rows.length === 0) {
            await conn.query(`UPDATE files SET status='empty' WHERE id=?`, [file.id]);
            continue;
          }

          const suggestedNameFromFile = path.basename(originalName || 'data', ext).toLowerCase();
          const tableName = sanitizeSqlIdentifier(explicitTableName || suggestedNameFromFile);

          const schema = inferTableSchema(parsed.headers, parsed.rows);
          const nullability = ((req.query.nullability as string) || 'all-nullable') as 'all-nullable' | 'inferred';
          const createSql = generateCreateTableSql(tableName, schema, ifExists, nullability);

          try {
            if (ifExists === 'replace') {
              await conn.query(`DROP TABLE IF EXISTS \`${tableName}\``);
            }
            if (ifExists === 'fail') {
              const [existing] = await conn.query(`SHOW TABLES LIKE ?`, [tableName]);
              const exists = Array.isArray(existing) && existing.length > 0;
              if (exists) {
                await conn.query(`UPDATE files SET status='failed' WHERE id=?`, [file.id]);
                continue;
              }
            }

            await conn.query(createSql);

            // Insert rows in batches
            const batchSize = 500;
            const columnNames = schema.columns.map(c => c.name);

            for (let i = 0; i < parsed.rows.length; i += batchSize) {
              const batch = parsed.rows.slice(i, i + batchSize);
              const values = batch.map(row => mapRowToSqlColumns(row, parsed.headers, schema));
              const placeholders = values.map(v => `(${new Array(v.length).fill('?').join(',')})`).join(',');
              const flatValues = values.flat();
              const insertSql = `INSERT INTO \`${tableName}\` (${columnNames.map(n => `\`${n}\``).join(',')}) VALUES ${placeholders}`;
              if (flatValues.length > 0) {
                try {
                  await conn.query(insertSql, flatValues);
                } catch (e: any) {
                  const msg: string = e?.sqlMessage || e?.message || '';
                  const m = msg.match(/Column '([^']+)' cannot be null/i);
                  if (m && m[1]) {
                    const colName = m[1];
                    const col = schema.columns.find(c => c.name === colName);
                    let typeStr = col ? (col.type === 'VARCHAR' && col.length ? `VARCHAR(${col.length})` : col.type) : '';
                    if (!typeStr) {
                      const [descRows] = await conn.query<any[]>(`DESCRIBE \`${tableName}\``);
                      const found = Array.isArray(descRows) ? (descRows as any[]).find(r => r.Field === colName) : null;
                      typeStr = found?.Type || 'TEXT';
                    }
                    await conn.query(`ALTER TABLE \`${tableName}\` MODIFY \`${colName}\` ${typeStr} NULL`);
                    await conn.query(insertSql, flatValues);
                  } else {
                    throw e;
                  }
                }
              }
            }

            // Mark as processed
            await conn.query(`UPDATE files SET row_count=?, status='processed' WHERE id=?`, [
              parsed.rows.length,
              file.id,
            ]);

            processedResults.push({
              fileId: file.id,
              table: tableName,
              rowCount: parsed.rows.length,
              status: 'processed',
            });

          } catch (err) {
            await conn.query(`UPDATE files SET status='failed' WHERE id=?`, [file.id]);
            console.error(`Error processing file ${file.name}:`, err);
          }
        }
      } finally {
        conn.release();
      }

      res.json({
        message: "Fetched and processed all files successfully",
        totalFiles: files.length,
        processed: processedResults,
      });
    }
  } catch (err: any) {
    console.error(err);
    res.status(500).json({ error: err.message || 'Internal server error' });
  } finally {
    if (req.file?.path && fs.existsSync(req.file.path)) {
      fs.unlink(req.file.path, () => void 0);
    }
    for (const p of cleanupPaths) {
      if (p && fs.existsSync(p)) {
        fs.unlink(p, () => void 0);
      }
    }
  }
});


// Admin APIs
app.get('/api/tables', async (_req: Request, res: Response) => {
  try {
    await ensureDatabaseConnection();
    const pool = await getDatabasePool();
    const [rows] = await pool.query<any[]>(`SHOW TABLES`);
    const dbName = process.env.DB_DATABASE as string;
    const key = `Tables_in_${dbName}`;
    const list = Array.isArray(rows)
      ? rows.map((r: any) => r[key] || Object.values(r)[0])
      : [];
    res.json({ tables: list });
  } catch (e: any) {
    res.status(500).json({ error: e.message || 'Failed to list tables' });
  }
});

app.get('/api/tables/:table/columns', async (req: Request, res: Response) => {
  try {
    const table = sanitizeSqlIdentifier(req.params.table);
    const pool = await getDatabasePool();
    const [rows] = await pool.query<any[]>(`DESCRIBE \`${table}\``);
    res.json({ columns: rows });
  } catch (e: any) {
    res.status(500).json({ error: e.message || 'Failed to get columns' });
  }
});

app.get('/api/tables/:table/preview', async (req: Request, res: Response) => {
  try {
    const table = sanitizeSqlIdentifier(req.params.table);
    const limit = Math.min(Number(req.query.limit) || 50, 500);
    const pool = await getDatabasePool();
    const [rows] = await pool.query<any[]>(`SELECT * FROM \`${table}\` LIMIT ?`, [limit]);
    res.json({ rows });
  } catch (e: any) {
    res.status(500).json({ error: e.message || 'Failed to preview table' });
  }
});

app.delete('/api/tables/:table', async (req: Request, res: Response) => {
  try {
    const table = sanitizeSqlIdentifier(req.params.table);
    const pool = await getDatabasePool();
    await pool.query(`DROP TABLE IF EXISTS \`${table}\``);
    res.json({ ok: true });
  } catch (e: any) {
    res.status(500).json({ error: e.message || 'Failed to drop table' });
  }
});

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

// Row CRUD endpoints
// app.get('/api/tables/:table/rows', async (req: Request, res: Response) => {
//   try {
//     const table = sanitizeSqlIdentifier(req.params.table);
//     await ensureIdColumn(table);
//     const limit = Math.min(Number(req.query.limit) || 50, 1000);
//     const offset = Math.max(Number(req.query.offset) || 0, 0);
//     const pool = await getDatabasePool();
//     const [rows] = await pool.query<any[]>(`SELECT * FROM \`${table}\` ORDER BY \`_id\` DESC LIMIT ? OFFSET ?`, [limit, offset]);
//     res.json({ rows });
//   } catch (e: any) {
//     res.status(500).json({ error: e.message || 'Failed to fetch rows' });
//   }
// });

app.get('/api/tables/:table/rows', async (req: Request, res: Response) => {
  try {
    const table = sanitizeSqlIdentifier(req.params.table);
    await ensureIdColumn(table);

    const limit = Math.min(Number(req.query.limit) || 50, 1000);
    const offset = Math.max(Number(req.query.offset) || 0, 0);
    const status = req.query.status as string | undefined; // optional query param

    const pool = await getDatabasePool();

    let query = `SELECT * FROM \`${table}\``;
    const params: any[] = [];

    if (status) {
      query += ` WHERE status = ?`;
      params.push(status);
    }

    query += ` ORDER BY \`_id\` DESC LIMIT ? OFFSET ?`;
    params.push(limit, offset);

    const [rows] = await pool.query<any[]>(query, params);

    res.json({ rows });
  } catch (e: any) {
    res.status(500).json({ error: e.message || 'Failed to fetch rows' });
  }
});

app.get('/api/tables/:table/rows/:id', async (req: Request, res: Response) => {
  try {
    const table = sanitizeSqlIdentifier(req.params.table);
    await ensureIdColumn(table);
    const id = Number(req.params.id);
    const pool = await getDatabasePool();
    const [rows] = await pool.query<any[]>(`SELECT * FROM \`${table}\` WHERE \`_id\` = ?`, [id]);
    const row = Array.isArray(rows) ? rows[0] : null;
    if (!row) return res.status(404).json({ error: 'Not found' });
    res.json({ row });
  } catch (e: any) {
    res.status(500).json({ error: e.message || 'Failed to fetch row' });
  }
});

app.post('/api/tables/:table/rows', async (req: Request, res: Response) => {
  try {
    const table = sanitizeSqlIdentifier(req.params.table);
    await ensureIdColumn(table);
    const body = req.body || {};
    delete (body as any)._id;
    const keys = Object.keys(body);
    const values = keys.map(k => (body as any)[k]);
    if (!keys.length) return res.status(400).json({ error: 'No fields' });
    const pool = await getDatabasePool();
    const sql = `INSERT INTO \`${table}\` (${keys.map(k => `\`${sanitizeSqlIdentifier(k)}\``).join(',')}) VALUES (${new Array(keys.length).fill('?').join(',')})`;
    const [result]: any = await pool.query(sql, values);
    res.json({ ok: true, id: result.insertId });
  } catch (e: any) {
    res.status(500).json({ error: e.message || 'Failed to create row' });
  }
});

app.put('/api/tables/:table/rows/:id', async (req: Request, res: Response) => {
  try {
    const table = sanitizeSqlIdentifier(req.params.table);
    await ensureIdColumn(table);
    const id = Number(req.params.id);
    const body = req.body || {};
    delete (body as any)._id;
    const keys = Object.keys(body);
    const values = keys.map(k => (body as any)[k]);
    if (!keys.length) return res.status(400).json({ error: 'No fields' });
    const pool = await getDatabasePool();
    const set = keys.map(k => `\`${sanitizeSqlIdentifier(k)}\` = ?`).join(',');
    const sql = `UPDATE \`${table}\` SET ${set} WHERE \`_id\` = ?`;
    await pool.query(sql, [...values, id]);
    res.json({ ok: true });
  } catch (e: any) {
    res.status(500).json({ error: e.message || 'Failed to update row' });
  }
});

app.delete('/api/tables/:table/rows/:id', async (req: Request, res: Response) => {
  try {
    const table = sanitizeSqlIdentifier(req.params.table);
    await ensureIdColumn(table);
    const id = Number(req.params.id);
    const pool = await getDatabasePool();
    await pool.query(`DELETE FROM \`${table}\` WHERE \`_id\` = ?`, [id]);
    res.json({ ok: true });
  } catch (e: any) {
    res.status(500).json({ error: e.message || 'Failed to delete row' });
  }
});

app.post('/api/tables/:table/rows/:id/toggle-active', async (req: Request, res: Response) => {
  try {
    const table = sanitizeSqlIdentifier(req.params.table);
    await ensureIdColumn(table);
    await ensureActiveColumn(table);
    const id = Number(req.params.id);
    const pool = await getDatabasePool();
    await pool.query(`UPDATE \`${table}\` SET \`active\` = IFNULL(1-\`active\`, 0) WHERE \`_id\` = ?`, [id]);
    res.json({ ok: true });
  } catch (e: any) {
    res.status(500).json({ error: e.message || 'Failed to toggle active' });
  }
});
const port = process.env.PORT ? Number(process.env.PORT) : 4000;
app.listen(port, () => {
  console.log(`Data importer listening on http://localhost:${port}`);
});


