// fileImporter.ts
import path from "path";
import fs from "fs";
import { getDatabasePool } from "./db";
import { listFilesRecursively } from "./googleDrive";
import { downloadToTemp } from "./downloader";
import { parseCsvFile, parseExcelFile } from "./parsers";
import { generateCreateTableSql, inferTableSchema, mapRowToSqlColumns, sanitizeSqlIdentifier } from "./schemaInference";
// import { listFilesRecursively, downloadToTemp } from "";
// import { parseCsvFile, parseExcelFile } from "./services/parser";
// import { sanitizeSqlIdentifier, inferTableSchema, generateCreateTableSql, mapRowToSqlColumns } from "./services/schema";

interface ParsedData {
    headers: string[];
    rows: any[];
}

/**
 * Step 1. Register file URLs in `files` table
 */
export async function registerFileUrls(folderId: string) {
    const files = await listFilesRecursively(folderId);
    const pool = await getDatabasePool();
    const conn = await pool.getConnection();

    try {
        for (const file of files) {
            await conn.query(
                `INSERT INTO files (id, name, mime_type, web_view_link, folder_path, row_count, status) 
                 VALUES (?, ?, ?, ?, ?, ?, ?) 
                 ON DUPLICATE KEY UPDATE status='pending'`,
                [file.id, file.name, file.mimeType, file.webViewLink, file.folderPath, 0, "pending"]
            );
        }
        return files;
    } finally {
        conn.release();
    }
}

/**
 * Step 2. Download and parse file into rows
 */
export async function downloadAndParseFile(file: any, overrideType?: string): Promise<{ localPath: string, originalName: string, parsed: ParsedData | null }> {
    const dl = await downloadToTemp(file);
    const localPath = dl.filePath;
    const originalName = dl.filename;

    let ext = path.extname(originalName || "").toLowerCase();
    const overrideMap: Record<string, string> = { csv: ".csv", xlsx: ".xlsx", xls: ".xls" };
    if (![".csv", ".xlsx", ".xls"].includes(ext)) {
        if (overrideType && overrideMap[overrideType]) {
            ext = overrideMap[overrideType];
        }
    }

    let parsed: ParsedData | null = null;
    if (ext === ".csv") {
        parsed = await parseCsvFile(localPath);
    } else if (ext === ".xlsx" || ext === ".xls") {
        parsed = await parseExcelFile(localPath);
    }

    return { localPath, originalName, parsed };
}

/**
 * Step 3. Insert parsed data into MySQL table
 */
export async function importDataToTable(file: any, parsed: ParsedData, ifExists: "append" | "replace" | "fail", explicitTableName?: string) {
    const pool = await getDatabasePool();
    const conn = await pool.getConnection();

    try {
        const ext = path.extname(file.name).toLowerCase();
        const suggestedNameFromFile = path.basename(file.name || "data", ext).toLowerCase();
        const tableName = sanitizeSqlIdentifier(explicitTableName || suggestedNameFromFile);

        const schema = inferTableSchema(parsed.headers, parsed.rows);
        const nullability = "all-nullable";
        const createSql = generateCreateTableSql(tableName, schema, ifExists, nullability);

        if (ifExists === "replace") {
            await conn.query(`DROP TABLE IF EXISTS \`${tableName}\``);
        }
        if (ifExists === "fail") {
            const [existing] = await conn.query(`SHOW TABLES LIKE ?`, [tableName]);
            if (Array.isArray(existing) && existing.length > 0) {
                await conn.query(`UPDATE files SET status='failed' WHERE id=?`, [file.id]);
                return;
            }
        }

        await conn.query(createSql);

        // Batch insert
        const batchSize = 500;
        const columnNames = schema.columns.map(c => c.name);

        for (let i = 0; i < parsed.rows.length; i += batchSize) {
            const batch = parsed.rows.slice(i, i + batchSize);
            const values = batch.map(row => mapRowToSqlColumns(row, parsed.headers, schema));
            const placeholders = values.map(v => `(${new Array(v.length).fill("?").join(",")})`).join(",");
            const flatValues = values.flat();
            const insertSql = `INSERT INTO \`${tableName}\` (${columnNames.map(n => `\`${n}\``).join(",")}) VALUES ${placeholders}`;

            if (flatValues.length > 0) {
                await conn.query(insertSql, flatValues);
            }
        }

        await conn.query(`UPDATE files SET row_count=?, status='processed' WHERE id=?`, [parsed.rows.length, file.id]);
    } finally {
        conn.release();
    }
}

export async function registerUploadedFile(file: Express.Multer.File) {
    const pool = await getDatabasePool();
    const [result]: any = await pool.query(
        `INSERT INTO files (originalName, mimeType, localPath, status) VALUES (?, ?, ?, 'pending')`,
        [file.originalname, file.mimetype, file.path]
    );

    return {
        id: result.insertId,
        originalName: file.originalname,
        mimeType: file.mimetype,
        status: "pending"
    };
}