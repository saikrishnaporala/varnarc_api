import fs from 'fs';
import { parse } from 'csv-parse';
import xlsx from 'xlsx';

export interface ParsedData {
  headers: string[];
  rows: Record<string, any>[];
}

export async function parseCsvFile(filePath: string): Promise<ParsedData> {
  const fileContent = await fs.promises.readFile(filePath, 'utf8');

  return new Promise((resolve, reject) => {
    const headers: string[] = [];
    const rows: Record<string, any>[] = [];

    const parser = parse(fileContent, {
      columns: true,
      skip_empty_lines: true,
      trim: true
    });

    parser.on('readable', () => {
      let record: any;
      while ((record = parser.read()) !== null) {
        if (headers.length === 0) {
          for (const key of Object.keys(record)) headers.push(key);
        }
        rows.push(record);
      }
    });
    parser.on('error', reject);
    parser.on('end', () => resolve({ headers, rows }));
  });
}

export async function parseExcelFile(filePath: string): Promise<ParsedData> {
  const workbook = xlsx.readFile(filePath);
  const sheetName = workbook.SheetNames[0];
  if (!sheetName) {
    return { headers: [], rows: [] };
  }
  const sheet = workbook.Sheets[sheetName];
  const json = xlsx.utils.sheet_to_json<Record<string, any>>(sheet, { defval: null });
  const headers = json.length > 0 ? Object.keys(json[0]) : [];
  return { headers, rows: json };
}



