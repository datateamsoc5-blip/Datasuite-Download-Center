// server.js
import express from 'express';
import path from 'path';
import fs from 'fs';
import os from 'os';
import multer from 'multer';
import unzipper from 'unzipper';
import csvParser from 'csv-parser';
import { stringify } from 'csv-stringify';
import { pipeline } from 'stream/promises';
import { v4 as uuidv4 } from 'uuid';
import sanitize from 'sanitize-filename';

const app = express();
const PORT = process.env.PORT || 3000;

// Directories
const DATA_DIR = path.resolve('./data');
const UPLOADS_DIR = path.resolve('./uploads');

if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
if (!fs.existsSync(UPLOADS_DIR)) fs.mkdirSync(UPLOADS_DIR, { recursive: true });

// Multer disk storage for large uploads
const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, UPLOADS_DIR),
  filename: (req, file, cb) => {
    const safe = sanitize(file.originalname);
    const unique = `${Date.now()}-${uuidv4()}-${safe}`;
    cb(null, unique);
  }
});

// Accept up to 500MB zip
const upload = multer({
  storage,
  limits: { fileSize: 500 * 1024 * 1024 }, // 500 MB
  fileFilter: (req, file, cb) => {
    if (!file.originalname.toLowerCase().endsWith('.zip')) {
      return cb(new Error('Only .zip files are allowed'));
    }
    cb(null, true);
  }
});

app.use(express.static(path.join(process.cwd(), 'public')));

// --------- Configuration: DROP headers (by header name) ----------
const DROP_HEADERS = new Set([
  // C:I -> Number TO, Status, High Value, Sender ID, Sender Name, Sender Type, Sender
  'Number TO','Status','High Value','Sender ID','Sender Name','Sender Type','Sender',
  // L -> Receiver Name
  'Receiver Name',
  // O:U -> Station Type, Current Station, TO Order, Quantity, TO Direction, Volumetric Weight, Weight
  'Station Type','Current Station','TO Order','Quantity','TO Direction','Volumetric Weight','Weight',
  // Y:AB -> Line Hual Trip Number, Operator, Create Time, Complete Time
  'Line Hual Trip Number','Operator','Create Time','Complete Time',
  // AD:AH -> Driver Scan Time, Journey Type, Remark, Receive Status, TO Remark
  'Driver Scan Time','Journey Type','Remark','Receive Status','TO Remark'
]);

// POST /api/upload - accepts zip file and consolidates CSVs
app.post('/api/upload', upload.single('zipfile'), async (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No file uploaded' });

  const zipPath = req.file.path;
  const uploadFilename = req.file.filename;
  const guid = uuidv4();
  const consolidatedFilename = `consolidated-${guid}.csv`;
  const consolidatedPath = path.join(DATA_DIR, consolidatedFilename);

  // Distinct collectors (Set)
  const distinct = {
    currentStation: new Set(),
    receiverType: new Set(),
    journeyType: new Set(),
    receiveStatus: new Set()
  };

  const preview = [];
  const PREVIEW_LIMIT = 50;

  let canonicalHeaders = null; // array of header names to keep (after drop)
  let wroteHeader = false;

  // Helper to safely read zip and stream CSV entries
  try {
    // Use unzipper to open the zip file from filesystem
    const directory = await unzipper.Open.file(zipPath);

    // Filter files that end with .csv (case-insensitive)
    const csvEntries = directory.files.filter(f => f.path && f.type === 'File' && f.path.toLowerCase().endsWith('.csv'));

    if (csvEntries.length === 0) {
      // cleanup upload
      fs.unlinkSync(zipPath);
      return res.status(400).json({ error: 'No CSV files found inside the zip' });
    }

    // Create write streams: csv-stringify -> file write
    const outStream = fs.createWriteStream(consolidatedPath, { flags: 'w' });
    const csvStringifier = stringify({ header: true }); // we'll manage header row manually by writing first

    // Pipe stringifier to file
    csvStringifier.pipe(outStream);

    // Process each CSV entry sequentially (to keep memory low)
    for (const entry of csvEntries) {
      // entry.stream() returns a readable stream of the file content
      const entryStream = entry.stream();

      // Create parser
      await new Promise((resolve, reject) => {
        const parser = csvParser({ mapHeaders: ({ header, index }) => header });

        parser.on('headers', (headers) => {
          // On first CSV determine canonicalHeaders (headers to keep)
          if (!canonicalHeaders) {
            // Filter out the DROP_HEADERS (preserve order)
            canonicalHeaders = headers.filter(h => !DROP_HEADERS.has(h));
            // write header to stringifier (csv-stringify will output them)
            csvStringifier.write(Object.fromEntries(canonicalHeaders.map(h => [h, h])));
            wroteHeader = true;
          }
        });

        parser.on('data', (row) => {
          // Ensure canonicalHeaders exist (some CSV might have missing headers)
          if (!canonicalHeaders) return; // defensive: skip until canonical headers known

          // Build output row using canonical headers
          const outRow = {};
          for (const h of canonicalHeaders) {
            outRow[h] = row[h] !== undefined ? row[h] : '';
          }

          // Collect distinct values for required selectors.
          // Use original row keys for 'Receiver type' because some CSVs may have slightly different casing;
          // prefer canonical/header present values, otherwise fallback to common variants.
          const pick = (rowObj, possibleNames) => {
            for (const k of possibleNames) {
              if (rowObj[k] !== undefined && rowObj[k] !== '') return rowObj[k];
            }
            return '';
          };

          // Current Station -> prefer outRow which is canonical (if it existed or left blank)
          const currentStationVal = outRow['Current Station'] || pick(row, ['Current Station', 'current station', 'current_station']);
          const receiverTypeVal = pick(row, ['Receiver type', 'Receiver Type', 'receiver type', 'receiver_type', 'Receiver type']);
          const journeyTypeVal = pick(row, ['Journey Type', 'Journey type', 'journey type']);
          const receiveStatusVal = pick(row, ['Receive Status', 'Receive status', 'receive status']);

          if (currentStationVal) distinct.currentStation.add(currentStationVal);
          if (receiverTypeVal) distinct.receiverType.add(receiverTypeVal);
          if (journeyTypeVal) distinct.journeyType.add(journeyTypeVal);
          if (receiveStatusVal) distinct.receiveStatus.add(receiveStatusVal);

          // Save preview rows (shallow) up to limit
          if (preview.length < PREVIEW_LIMIT) preview.push(outRow);

          // Write outRow to consolidated CSV
          csvStringifier.write(outRow);
        });

        parser.on('end', () => resolve());
        parser.on('error', (err) => reject(err));

        // Pipe entry stream into parser
        entryStream.pipe(parser);
      });
    }

    // finalise stringifier
    csvStringifier.end();

    // Wait for outStream to finish
    await new Promise((resolve, reject) => outStream.on('finish', resolve).on('error', reject));

    // cleanup uploaded zip
    try { fs.unlinkSync(zipPath); } catch (e) { /* ignore */ }

    // Convert distinct sets to arrays, filtered and limited (prevent huge payloads)
    const toArrayLimit = (s, limit = 1000) => Array.from(s).filter(x => x !== undefined && x !== null && x !== '').slice(0, limit);

    return res.json({
      preview,
      distinct: {
        currentStation: toArrayLimit(distinct.currentStation, 1000),
        receiverType: toArrayLimit(distinct.receiverType, 1000),
        journeyType: toArrayLimit(distinct.journeyType, 1000),
        receiveStatus: toArrayLimit(distinct.receiveStatus, 1000)
      },
      consolidatedPath: `/data/${path.basename(consolidatedPath)}`
    });

  } catch (err) {
    console.error('Error during consolidation:', err);
    // try cleanup
    try { if (fs.existsSync(zipPath)) fs.unlinkSync(zipPath); } catch (e) {}
    return res.status(500).json({ error: String(err) });
  }
});

// Serve consolidated files
app.use('/data', express.static(DATA_DIR));

// GET /api/preview?file=<consolidatedFilename>&limit=50
app.get('/api/preview', async (req, res) => {
  const file = req.query.file;
  const limit = Number(req.query.limit || 50);
  if (!file) return res.status(400).json({ error: 'file query param required' });

  const safe = path.basename(file); // avoids directory traversal
  const abs = path.join(DATA_DIR, safe);
  if (!fs.existsSync(abs)) return res.status(404).json({ error: 'file not found' });

  const out = [];
  fs.createReadStream(abs)
    .pipe(csvParser())
    .on('data', (row) => {
      if (out.length < limit) out.push(row);
    })
    .on('end', () => res.json({ preview: out }))
    .on('error', (err) => res.status(500).json({ error: String(err) }));
});

// Simple health and index route
app.get('/', (req, res) => res.sendFile(path.join(process.cwd(), 'public', 'index.html')));

app.listen(PORT, () => {
  console.log(`Server listening on http://localhost:${PORT}`);
});
