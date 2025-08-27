import { Router, Request, Response } from "express";
import path from "path";
import fs from "fs/promises";
import { getDatabasePool } from "../services/db";
import { downloadAndParseFile, importDataToTable } from "../services/fileImporter";

const router = Router();

// Upload API
/**
 * @openapi
 * /api/files/{id}/upload:
 *   post:
 *     summary: Upload a file for a given ID
 *     tags:
 *       - Files
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema:
 *           type: string
 *         description: The file ID
 *     requestBody:
 *       required: true
 *       content:
 *         multipart/form-data:
 *           schema:
 *             type: object
 *             properties:
 *               file:
 *                 type: string
 *                 format: binary
 *     responses:
 *       200:
 *         description: File uploaded successfully
 */
router.post("/:id/upload", async (req: Request, res: Response) => {
    const pool = await getDatabasePool();
    const { id } = req.params;

    try {
        // Get file entry from DB
        const [rows]: any = await pool.query(
            `SELECT * FROM files WHERE id = ?`,
            [id]
        );

        if (!rows.length) {
            return res.status(404).json({ error: "File not found" });
        }

        const file = rows[0];

        if (file.status === "success") {
            return res.status(400).json({ error: "File already uploaded" });
        }
        let downloadfile;
        try {
            downloadfile = await downloadAndParseFile(file.web_view_link);
            if (!downloadfile || !downloadfile.parsed) {
                return res.status(400).json({
                    error: "Failed to parse file",
                    details: `Parser returned null for ${file.web_view_link}`,
                });
            }

            const dataStatus = await importDataToTable(
                file,
                downloadfile.parsed,
                "append",
                ""
            );

            res.json({
                message: "File uploaded successfully",
                fileId: dataStatus,
            });
        } catch (err: any) {
            console.error("Error while downloading file:", err);
            return res.status(400).json({
                error: `Failed to download file from ${file.web_view_link}`,
                details: err?.message || String(err),
            });
        }

        // TODO: Replace with actual upload logic (e.g., S3, FTP, etc.)
        console.log(`Uploading file: ${downloadfile}`);

        // Update DB status
        // await pool.query(
        //   `UPDATE files SET status = 'success' WHERE id = ?`,
        //   [id]
        // );

        // res.json({ message: "File uploaded successfully", fileId: downloadfile });
    } catch (err) {
        console.error("Error in file upload API:", err);
        res.status(500).json({ error: "Internal server error" });
    }
});

export default router;
