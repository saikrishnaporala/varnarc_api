import fs from 'fs';
import path from 'path';
import axios from 'axios';

type AxiosConfig = Parameters<typeof axios>[0]; 

/**
 * Downloads a remote file to a temporary location and returns the file path and suggested name.
 * Supports direct URLs. For Google Drive share links, pass the direct download URL when possible.
 */
export async function downloadToTemp(url: string): Promise<{ filePath: string; filename: string }> {
  url = normalizeUrl(url);
  const tempDir = path.join(process.cwd(), "downloads");
  if (!fs.existsSync(tempDir)) fs.mkdirSync(tempDir, { recursive: true });

  console.log("URL:", url);

  const config: AxiosConfig = {
    url,
    method: "GET",
    responseType: "stream",
    headers: {
      "User-Agent": "Mozilla/5.0 (DataImporter)",
    },
    validateStatus: (s) => !!s && s >= 200 && s < 400,
  };

  const resp = await axios(config);

  // Handle filename from Content-Disposition header
  const disp = resp.headers["content-disposition"] as string | undefined;
  let suggested = "remote_file";
  if (disp) {
    const match = /filename\*=UTF-8''([^;]+)|filename="?([^";]+)"?/i.exec(disp);
    const fn = decodeURIComponent((match?.[1] || match?.[2] || "").trim());
    if (fn) suggested = fn;
  } else {
    const urlPath = new URL(url).pathname;
    const base = path.basename(urlPath);
    if (base && base !== "/" && base !== "") suggested = base;
  }

  // Infer extension if missing
  const contentType = String(resp.headers["content-type"] || "").toLowerCase();
  const ext = path.extname(suggested).toLowerCase();
  if (!ext) {
    if (contentType.includes("text/csv") || contentType.includes("application/csv")) {
      suggested += ".csv";
    } else if (contentType.includes("sheet") || contentType.includes("excel")) {
      suggested += ".xlsx";
    }
  }

  // Generate unique filename
  const unique = `${Date.now()}-${Math.round(Math.random() * 1e9)}`;
  const finalFilename = `${unique}-${suggested}`;
  const filePath = path.join(tempDir, finalFilename);

  // Cast resp.data to stream
  const stream = resp.data as NodeJS.ReadableStream;
  const writer = fs.createWriteStream(filePath);

  await new Promise<void>((resolve, reject) => {
    stream.pipe(writer);
    writer.on("finish", resolve);
    writer.on("error", reject);
  });

  return { filePath, filename: finalFilename };
}

function normalizeUrl(input: string): string {
  try {
    const u = new URL(input);
    // Handle Google Sheets: convert to CSV export URL
    if (u.hostname.includes('docs.google.com') && u.pathname.includes('/spreadsheets/')) {
      // Expect /spreadsheets/d/<ID>/...
      const m = u.pathname.match(/\/spreadsheets\/d\/([^/]+)/);
      const id = m?.[1] || '';
      // gid can be in query or hash
      let gid = u.searchParams.get('gid') || '';
      if (!gid && u.hash) {
        const hash = u.hash.replace(/^#/, '');
        const params = new URLSearchParams(hash);
        gid = params.get('gid') || '';
      }
      if (id) {
        const base = `https://docs.google.com/spreadsheets/d/${id}/export?format=csv`;
        return gid ? `${base}&gid=${gid}` : base;
      }
    }
    if (u.hostname.includes('drive.google.com')) {
      // Cases: /file/d/<id>/view, /open?id=<id>, /uc?id=<id>
      let id = '';
      const m = u.pathname.match(/\/file\/d\/([^/]+)/);
      if (m && m[1]) id = m[1];
      if (!id) id = u.searchParams.get('id') || '';
      if (id) {
        return `https://drive.google.com/uc?export=download&id=${id}`;
      }
    }
    return input;
  } catch {
    return input;
  }
}


