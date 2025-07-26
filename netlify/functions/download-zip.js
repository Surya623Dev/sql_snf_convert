const JSZip = require('jszip');

exports.handler = async (event, context) => {
  // Set CORS headers
  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
  };

  // Handle preflight requests
  if (event.httpMethod === 'OPTIONS') {
    return {
      statusCode: 200,
      headers,
      body: '',
    };
  }

  if (event.httpMethod !== 'POST') {
    return {
      statusCode: 405,
      headers,
      body: JSON.stringify({ error: 'Method not allowed' }),
    };
  }

  try {
    const { files } = JSON.parse(event.body);
    
    if (!files || !Array.isArray(files)) {
      return {
        statusCode: 400,
        headers,
        body: JSON.stringify({ error: 'Invalid files data' }),
      };
    }

    const zip = new JSZip();
    
    files.forEach(file => {
      if (file.convertedSql && file.status === 'success') {
        const filename = file.filename.replace(/\.sql$/i, '_snowflake.sql');
        zip.file(filename, file.convertedSql);
      }
    });

    const zipBuffer = await zip.generateAsync({ type: 'nodebuffer' });
    
    return {
      statusCode: 200,
      headers: {
        ...headers,
        'Content-Type': 'application/zip',
        'Content-Disposition': 'attachment; filename=converted_sql_files.zip',
      },
      body: zipBuffer.toString('base64'),
      isBase64Encoded: true,
    };
  } catch (error) {
    console.error('ZIP creation error:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ error: 'Failed to create ZIP file' }),
    };
  }
};