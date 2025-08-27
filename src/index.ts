import express, { Request, Response } from 'express';
import cors from 'cors';
import path from 'path';
import fs from 'fs';
import dotenv from 'dotenv';
import fileRoutes from "./routes/files";
import googleurls from "./routes/google_urls";
import swaggerUi from "swagger-ui-express";
import swaggerJsdoc, { Options } from "swagger-jsdoc";

dotenv.config();

const app = express();

app.use(cors());
app.use(express.json());

app.use(express.json({ limit: '10mb' }));

// Swagger options
const options: Options = {
  definition: {
    openapi: "3.0.0",
    info: {
      title: "Varnarc API",
      version: "1.0.0",
      description: "Express API with Swagger UI (TypeScript example)",
    },
    servers: [{ url: `http://localhost:4000` }],
  },
  apis: ["./src/routes/*.ts"],
};

const swaggerSpec = swaggerJsdoc(options);

// Swagger UI
app.use("/docs", swaggerUi.serve, swaggerUi.setup(swaggerSpec));

app.listen(4000, () => {
  console.log("Server running at http://localhost:4000");
  console.log("Swagger docs available at http://localhost:4000/docs");
});

app.use("/api/files", fileRoutes);
app.use("/api/google_urls", googleurls);

app.use('/admin', express.static(path.join(process.cwd(), 'public', 'admin')));

const uploadDir = path.join(process.cwd(), 'uploads');
if (!fs.existsSync(uploadDir)) {
  fs.mkdirSync(uploadDir, { recursive: true });
}

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

const port = process.env.PORT ? Number(process.env.PORT) : 4000;
app.listen(port, () => {
  console.log(`Data importer listening on http://localhost:${port}`);
});
