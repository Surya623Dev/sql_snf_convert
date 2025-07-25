import express from 'express';
import multer from 'multer';
import path from 'path';
import { fileURLToPath } from 'url';
import fs from 'fs';
import JSZip from 'jszip';
import cors from 'cors';
import { convertToSnowflake } from './converter.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const port = 3001;

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.static('dist'));

// Configure multer for file uploads
const storage = multer.memoryStorage();
const upload = multer({ 
  storage,
  fileFilter: (req, file, cb) => {
    if (file.originalname.toLowerCase().endsWith('.sql')) {
      cb(null, true);
    } else {
      cb(new Error('Only .sql files are allowed'), false);
    }
  },
  limits: {
    fileSize: 10 * 1024 * 1024 // 10MB limit
  }
});

// Routes
app.post('/api/convert', upload.array('files'), async (req, res) => {
  try {
    const files = req.files;
    if (!files || files.length === 0) {
      return res.status(400).json({ error: 'No files uploaded' });
    }

    const results = await Promise.allSettled(
      files.map(async (file) => {
        const originalSql = file.buffer.toString('utf-8');
        const sourceDialect = req.body.sourceDialect || 'mysql';
        
        try {
          const convertedSql = await convertToSnowflake(originalSql, sourceDialect);
          return {
            filename: file.originalname,
            originalSql,
            convertedSql,
            status: 'success',
            size: file.size
          };
        } catch (conversionError) {
          return {
            filename: file.originalname,
            originalSql,
            convertedSql: null,
            status: 'error',
            error: conversionError.message,
            size: file.size
          };
        }
      })
    );

    const processedFiles = results.map((result, index) => {
      if (result.status === 'fulfilled') {
        return result.value;
      } else {
        return {
          filename: files[index].originalname,
          originalSql: files[index].buffer.toString('utf-8'),
          convertedSql: null,
          status: 'error',
          error: result.reason?.message || 'Conversion failed',
          size: files[index].size
        };
      }
    });

    res.json({ files: processedFiles });
  } catch (error) {
    console.error('Conversion error:', error);
    res.status(500).json({ error: 'Internal server error during conversion' });
  }
});

app.post('/api/download-zip', express.json(), async (req, res) => {
  try {
    const { files } = req.body;
    
    if (!files || !Array.isArray(files)) {
      return res.status(400).json({ error: 'Invalid files data' });
    }

    const zip = new JSZip();
    
    files.forEach(file => {
      if (file.convertedSql && file.status === 'success') {
        const filename = file.filename.replace(/\.sql$/i, '_snowflake.sql');
        zip.file(filename, file.convertedSql);
      }
    });

    const zipBuffer = await zip.generateAsync({ type: 'nodebuffer' });
    
    res.set({
      'Content-Type': 'application/zip',
      'Content-Disposition': 'attachment; filename=converted_sql_files.zip',
      'Content-Length': zipBuffer.length
    });
    
    res.send(zipBuffer);
  } catch (error) {
    console.error('ZIP creation error:', error);
    res.status(500).json({ error: 'Failed to create ZIP file' });
  }
});

app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, '../dist/index.html'));
});

app.listen(port, () => {
  console.log(`Server running at http://localhost:${port}`);
});