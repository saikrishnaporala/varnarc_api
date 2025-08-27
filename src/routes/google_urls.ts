import { Router, Request, Response } from "express";
import path from "path";
import fs from 'fs';
import { ensureDatabaseConnection, getDatabasePool } from "../services/db";
import { downloadAndParseFile, importDataToTable, registerFileUrls, registerUploadedFile } from "../services/fileImporter";
import multer from "multer";
import { downloadToTemp } from "../services/downloader";
import { listFilesRecursively } from "../services/googleDrive";
import { ParsedData, parseCsvFile, parseExcelFile } from "../services/parsers";
import { sanitizeSqlIdentifier, inferTableSchema, generateCreateTableSql, mapRowToSqlColumns } from "../services/schemaInference";

const router = Router();
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

/**
 * @openapi
 * /upload:
 *   post:
 *     summary: Upload a file or register a Google Drive folder
 *     description: >
 *       Upload a file (via multipart/form-data) or register files from a Google Drive folder URL.
 *       - If uploading directly, pass a file in field **file**.
 *       - If registering from Google Drive, provide a `fileUrl` in query/body.
 *     tags:
 *       - Google URLs Parsing
 *     parameters:
 *       - in: query
 *         name: ifExists
 *         required: false
 *         schema:
 *           type: string
 *           enum: [append, replace, fail]
 *         description: "Behavior if table already exists (default: append)."
 *       - in: query
 *         name: tableName
 *         required: false
 *         schema:
 *           type: string
 *         description: "Explicit table name to import into."
 *       - in: query
 *         name: fileUrl
 *         required: false
 *         schema:
 *           type: string
 *         description: "Google Drive folder URL (alternative to file upload)."
 *     requestBody:
 *       required: false
 *       content:
 *         multipart/form-data:
 *           schema:
 *             type: object
 *             properties:
 *               file:
 *                 type: string
 *                 format: binary
 *                 description: "File to upload."
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               fileUrl:
 *                 type: string
 *                 description: "Google Drive folder URL."
 *     responses:
 *       200:
 *         description: "File registered successfully."
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 message:
 *                   type: string
 *                 registered:
 *                   type: object
 *                   properties:
 *                     id:
 *                       type: string
 *                     name:
 *                       type: string
 *                     status:
 *                       type: string
 *                 files:
 *                   type: array
 *                   items:
 *                     type: object
 *       400:
 *         description: "Bad request (missing file or invalid fileUrl)."
 *       500:
 *         description: "Internal server error."
 */
router.post('/upload', upload.single('file'), async (req: Request, res: Response) => {
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

/**
 * @openapi
 * /upload_old:
 *   post:
 *     summary: Upload or fetch files (legacy version)
 *     description: >
 *       Legacy endpoint that uploads a file (via multipart/form-data) **or**
 *       fetches and processes files from a Google Drive folder URL.
 *       - If uploading directly, pass a file in field **file**.
 *       - If using Google Drive, provide a `fileUrl` in query/body.
 *     tags:
 *       - Google URLs Parsing
 *     parameters:
 *       - in: query
 *         name: ifExists
 *         required: false
 *         schema:
 *           type: string
 *           enum: [append, replace, fail]
 *         description: "Behavior if table already exists (default: append)."
 *       - in: query
 *         name: tableName
 *         required: false
 *         schema:
 *           type: string
 *         description: "Explicit table name to import into."
 *       - in: query
 *         name: fileUrl
 *         required: false
 *         schema:
 *           type: string
 *         description: "Google Drive folder URL (alternative to file upload)."
 *       - in: query
 *         name: fileType
 *         required: false
 *         schema:
 *           type: string
 *           enum: [csv, xlsx, xls]
 *         description: "Force file type if not detected."
 *       - in: query
 *         name: nullability
 *         required: false
 *         schema:
 *           type: string
 *           enum: [all-nullable, inferred]
 *         description: "How to treat nullability when creating schema."
 *     requestBody:
 *       required: false
 *       content:
 *         multipart/form-data:
 *           schema:
 *             type: object
 *             properties:
 *               file:
 *                 type: string
 *                 format: binary
 *                 description: "File to upload."
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               fileUrl:
 *                 type: string
 *                 description: "Google Drive folder URL."
 *     responses:
 *       200:
 *         description: "Files fetched/processed successfully."
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 message:
 *                   type: string
 *                 totalFiles:
 *                   type: integer
 *                 processed:
 *                   type: array
 *                   items:
 *                     type: object
 *                     properties:
 *                       fileId:
 *                         type: string
 *                       table:
 *                         type: string
 *                       rowCount:
 *                         type: integer
 *                       status:
 *                         type: string
 *       400:
 *         description: "Bad request (missing file or invalid fileUrl)."
 *       500:
 *         description: "Internal server error."
 */
router.post('/upload_old', upload.single('file'), async (req: Request, res: Response) => {
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

/**
 * @openapi
 * /api/tables:
 *   get:
 *     summary: List all tables in the database
 *     tags: [Tables]
 *     responses:
 *       200:
 *         description: List of database tables
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 tables:
 *                   type: array
 *                   items:
 *                     type: string
 */
router.get('/api/tables', async (_req: Request, res: Response) => {
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

/**
 * @openapi
 * /api/tables/{table}/columns:
 *   get:
 *     summary: Get column definitions of a table
 *     tags: [Tables]
 *     parameters:
 *       - in: path
 *         name: table
 *         required: true
 *         schema:
 *           type: string
 *         description: Table name
 *     responses:
 *       200:
 *         description: Column metadata
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 columns:
 *                   type: array
 *                   items:
 *                     type: object
 */
router.get('/api/tables/:table/columns', async (req: Request, res: Response) => {
    try {
        const table = sanitizeSqlIdentifier(req.params.table);
        const pool = await getDatabasePool();
        const [rows] = await pool.query<any[]>(`DESCRIBE \`${table}\``);
        res.json({ columns: rows });
    } catch (e: any) {
        res.status(500).json({ error: e.message || 'Failed to get columns' });
    }
});

/**
 * @openapi
 * /api/tables/{table}/preview:
 *   get:
 *     summary: Preview table rows
 *     tags: [Tables]
 *     parameters:
 *       - in: path
 *         name: table
 *         required: true
 *         schema:
 *           type: string
 *       - in: query
 *         name: limit
 *         schema:
 *           type: integer
 *           default: 50
 *     responses:
 *       200:
 *         description: Table rows preview
 */
router.get('/api/tables/:table/preview', async (req: Request, res: Response) => {
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

/**
 * @openapi
 * /api/tables/{table}:
 *   delete:
 *     summary: Drop a table
 *     tags: [Tables]
 *     parameters:
 *       - in: path
 *         name: table
 *         required: true
 *         schema:
 *           type: string
 *     responses:
 *       200:
 *         description: Table dropped successfully
 */
router.delete('/api/tables/:table', async (req: Request, res: Response) => {
    try {
        const table = sanitizeSqlIdentifier(req.params.table);
        const pool = await getDatabasePool();
        await pool.query(`DROP TABLE IF EXISTS \`${table}\``);
        res.json({ ok: true });
    } catch (e: any) {
        res.status(500).json({ error: e.message || 'Failed to drop table' });
    }
});



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

/**
 * @openapi
 * /api/tables/{table}/rows:
 *   get:
 *     summary: List rows from a table
 *     tags: [Rows]
 *     parameters:
 *       - in: path
 *         name: table
 *         required: true
 *         schema:
 *           type: string
 *       - in: query
 *         name: limit
 *         schema:
 *           type: integer
 *           default: 50
 *       - in: query
 *         name: offset
 *         schema:
 *           type: integer
 *       - in: query
 *         name: status
 *         schema:
 *           type: string
 *     responses:
 *       200:
 *         description: List of rows
 *
 *   post:
 *     summary: Insert a new row
 *     tags: [Rows]
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             additionalProperties: true
 *     responses:
 *       200:
 *         description: Row created
 */
router.get('/api/tables/:table/rows', async (req: Request, res: Response) => {
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

/**
 * @openapi
 * /api/tables/{table}/rows/{id}:
 *   get:
 *     summary: Get a row by ID
 *     tags: [Rows]
 *     parameters:
 *       - in: path
 *         name: table
 *         required: true
 *         schema:
 *           type: string
 *       - in: path
 *         name: id
 *         required: true
 *         schema:
 *           type: integer
 *     responses:
 *       200:
 *         description: Single row
 *
 *   put:
 *     summary: Update a row
 *     tags: [Rows]
 *     parameters:
 *       - in: path
 *         name: table
 *         required: true
 *       - in: path
 *         name: id
 *         required: true
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             additionalProperties: true
 *     responses:
 *       200:
 *         description: Row updated
 *
 *   delete:
 *     summary: Delete a row
 *     tags: [Rows]
 *     parameters:
 *       - in: path
 *         name: table
 *         required: true
 *       - in: path
 *         name: id
 *         required: true
 *     responses:
 *       200:
 *         description: Row deleted
 */
router.get('/api/tables/:table/rows/:id', async (req: Request, res: Response) => {
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

/**
 * @openapi
 * /api/tables/{table}/rows/{id}/toggle-active:
 *   post:
 *     summary: Toggle the `active` column for a row
 *     tags: [Rows]
 *     parameters:
 *       - in: path
 *         name: table
 *         required: true
 *       - in: path
 *         name: id
 *         required: true
 *         schema:
 *           type: integer
 *     responses:
 *       200:
 *         description: Active column toggled
 */
router.post('/api/tables/:table/rows', async (req: Request, res: Response) => {
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

router.put('/api/tables/:table/rows/:id', async (req: Request, res: Response) => {
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

router.delete('/api/tables/:table/rows/:id', async (req: Request, res: Response) => {
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

router.post('/api/tables/:table/rows/:id/toggle-active', async (req: Request, res: Response) => {
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
export default router;

function ensureIdColumn(table: string) {
    throw new Error("Function not implemented.");
}


function ensureActiveColumn(table: string) {
    throw new Error("Function not implemented.");
}
