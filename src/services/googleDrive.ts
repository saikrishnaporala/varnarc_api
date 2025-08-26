import { google } from "googleapis";
import path from "path";

const SERVICE_ACCOUNT_FILE = path.join(process.cwd(), "varnarc-googleapi.json"); // your key
const SCOPES = ["https://www.googleapis.com/auth/drive.readonly"];

const auth = new google.auth.GoogleAuth({
    keyFile: SERVICE_ACCOUNT_FILE,
    scopes: SCOPES,
});

const drive = google.drive({ version: "v3", auth });

export async function listFilesInFolder(folderId: string) {
    const drive = google.drive({ version: "v3", auth });

    const res = await drive.files.list({
        q: `'${folderId}' in parents and trashed = false`,
        fields: "files(id, name, mimeType, webViewLink)",
    });

    return res.data.files || [];
}

export async function listFilesRecursively(folderId: string, parentPath = "root"): Promise<any[]> {
    let allFiles: any[] = [];
  
    async function traverse(folderId: string, currentPath: string) {
      const res = await drive.files.list({
        q: `'${folderId}' in parents and trashed = false`,
        fields: "files(id, name, mimeType, webViewLink)",
      });
  
      const files = res.data.files || [];
  
      for (const file of files) {
        if (file.mimeType === "application/vnd.google-apps.folder") {
          if (!file.id || !file.name) continue; // ✅ guard for undefined values
  
          const newPath = `${currentPath}/${file.name}`;
          console.log(`📂 Folder: ${file.name}`);
  
          await traverse(file.id, newPath); // ✅ now guaranteed string
        } else {
          if (!file.id || !file.name) continue; // ✅ skip invalid files
  
          console.log(`📄 File: ${file.name} in ${currentPath}`);
          allFiles.push({
            ...file,
            folderPath: currentPath,
          });
        }
      }
    }
  
    await traverse(folderId, parentPath);
    return allFiles;
  }

// export async function downloadFile(fileId: string): Promise<NodeJS.ReadableStream> {
//   const client = await auth.getClient();
//   const drive = google.drive({ version: "v3", auth: client });

//   const res = await drive.files.get(
//     { fileId, alt: "media" },
//     { responseType: "stream" }
//   );

//   return res.data as NodeJS.ReadableStream;
// }
