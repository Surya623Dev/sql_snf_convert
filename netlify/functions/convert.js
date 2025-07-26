const multipart = require('lambda-multipart-parser');

/**
 * SQL to Snowflake Converter
 * Handles conversion from various SQL dialects to Snowflake-compatible SQL
 */

class SQLConverter {
  constructor() {
    this.conversionRules = {
      mysql: [
        // Data type conversions
        { pattern: /\bTINYINT\b/gi, replacement: 'NUMBER(3,0)' },
        { pattern: /\bSMALLINT\b/gi, replacement: 'NUMBER(5,0)' },
        { pattern: /\bMEDIUMINT\b/gi, replacement: 'NUMBER(7,0)' },
        { pattern: /\bINT\b/gi, replacement: 'NUMBER(10,0)' },
        { pattern: /\bBIGINT\b/gi, replacement: 'NUMBER(19,0)' },
        { pattern: /\bDOUBLE\b/gi, replacement: 'FLOAT' },
        { pattern: /\bTEXT\b/gi, replacement: 'VARCHAR(16777216)' },
        { pattern: /\bLONGTEXT\b/gi, replacement: 'VARCHAR(16777216)' },
        { pattern: /\bMEDIUMTEXT\b/gi, replacement: 'VARCHAR(16777216)' },
        { pattern: /\bTINYTEXT\b/gi, replacement: 'VARCHAR(255)' },
        
        // Function conversions
        { pattern: /\bNOW\(\)/gi, replacement: 'CURRENT_TIMESTAMP()' },
        { pattern: /\bCURDATE\(\)/gi, replacement: 'CURRENT_DATE()' },
        { pattern: /\bCURTIME\(\)/gi, replacement: 'CURRENT_TIME()' },
        { pattern: /\bCONCAT\(/gi, replacement: 'CONCAT(' },
        { pattern: /\bIFNULL\(/gi, replacement: 'NVL(' },
        
        // Syntax conversions
        { pattern: /\bLIMIT\s+(\d+)/gi, replacement: 'LIMIT $1' },
        { pattern: /\b`([^`]+)`/g, replacement: '"$1"' }, // Backticks to double quotes
        { pattern: /\bAUTO_INCREMENT\b/gi, replacement: 'AUTOINCREMENT' },
        { pattern: /\bENGINE\s*=\s*\w+/gi, replacement: '' },
        { pattern: /\bCHARSET\s*=\s*\w+/gi, replacement: '' },
        { pattern: /\bCOLLATE\s*=\s*[\w_]+/gi, replacement: '' },
      ],
      
      postgresql: [
        // Data type conversions
        { pattern: /\bSERIAL\b/gi, replacement: 'NUMBER AUTOINCREMENT' },
        { pattern: /\bBIGSERIAL\b/gi, replacement: 'NUMBER AUTOINCREMENT' },
        { pattern: /\bBOOLEAN\b/gi, replacement: 'BOOLEAN' },
        { pattern: /\bTEXT\b/gi, replacement: 'VARCHAR(16777216)' },
        { pattern: /\bBYTEA\b/gi, replacement: 'BINARY' },
        
        // Function conversions
        { pattern: /\bNOW\(\)/gi, replacement: 'CURRENT_TIMESTAMP()' },
        { pattern: /\bCURRENT_DATE\b/gi, replacement: 'CURRENT_DATE()' },
        { pattern: /\bCURRENT_TIME\b/gi, replacement: 'CURRENT_TIME()' },
        { pattern: /\bCOALESCE\(/gi, replacement: 'COALESCE(' },
        
        // Syntax conversions
        { pattern: /\bLIMIT\s+(\d+)\s+OFFSET\s+(\d+)/gi, replacement: 'LIMIT $2, $1' },
        { pattern: /\bRETURNING\s+[\w\s,*]+/gi, replacement: '' },
      ],
      
      sqlserver: [
        // Data type conversions
        { pattern: /\bBIT\b/gi, replacement: 'BOOLEAN' },
        { pattern: /\bTINYINT\b/gi, replacement: 'NUMBER(3,0)' },
        { pattern: /\bSMALLINT\b/gi, replacement: 'NUMBER(5,0)' },
        { pattern: /\bINT\b/gi, replacement: 'NUMBER(10,0)' },
        { pattern: /\bBIGINT\b/gi, replacement: 'NUMBER(19,0)' },
        { pattern: /\bREAL\b/gi, replacement: 'FLOAT4' },
        { pattern: /\bFLOAT\b/gi, replacement: 'FLOAT8' },
        { pattern: /\bNVARCHAR\(MAX\)/gi, replacement: 'VARCHAR(16777216)' },
        { pattern: /\bVARCHAR\(MAX\)/gi, replacement: 'VARCHAR(16777216)' },
        { pattern: /\bTEXT\b/gi, replacement: 'VARCHAR(16777216)' },
        { pattern: /\bNTEXT\b/gi, replacement: 'VARCHAR(16777216)' },
        
        // Function conversions
        { pattern: /\bGETDATE\(\)/gi, replacement: 'CURRENT_TIMESTAMP()' },
        { pattern: /\bGETUTCDATE\(\)/gi, replacement: 'CURRENT_TIMESTAMP()' },
        { pattern: /\bISNULL\(/gi, replacement: 'NVL(' },
        { pattern: /\bLEN\(/gi, replacement: 'LENGTH(' },
        
        // Syntax conversions
        { pattern: /\bTOP\s+(\d+)/gi, replacement: 'LIMIT $1' },
        { pattern: /\bIDENTITY\([\d,\s]+\)/gi, replacement: 'AUTOINCREMENT' },
        { pattern: /\b\[([^\]]+)\]/g, replacement: '"$1"' }, // Square brackets to double quotes
      ],
      
      oracle: [
        // Data type conversions
        { pattern: /\bNUMBER\b/gi, replacement: 'NUMBER' },
        { pattern: /\bVARCHAR2\b/gi, replacement: 'VARCHAR' },
        { pattern: /\bNCHAR\b/gi, replacement: 'CHAR' },
        { pattern: /\bNVARCHAR2\b/gi, replacement: 'VARCHAR' },
        { pattern: /\bCLOB\b/gi, replacement: 'VARCHAR(16777216)' },
        { pattern: /\bBLOB\b/gi, replacement: 'BINARY' },
        { pattern: /\bDATE\b/gi, replacement: 'TIMESTAMP_NTZ' },
        
        // Function conversions
        { pattern: /\bSYSDATE\b/gi, replacement: 'CURRENT_TIMESTAMP()' },
        { pattern: /\bNVL\(/gi, replacement: 'NVL(' },
        { pattern: /\bNVL2\(/gi, replacement: 'IFF(' },
        { pattern: /\bDECODE\(/gi, replacement: 'CASE' },
        
        // Syntax conversions
        { pattern: /\bROWNUM\s*<=?\s*(\d+)/gi, replacement: 'LIMIT $1' },
        { pattern: /\bDUAL\b/gi, replacement: '(SELECT 1)' },
      ]
    };
  }

  async convertToSnowflake(sql, sourceDialect = 'mysql') {
    if (!sql || typeof sql !== 'string') {
      throw new Error('Invalid SQL input');
    }

    let convertedSql = sql.trim();
    const rules = this.conversionRules[sourceDialect.toLowerCase()] || this.conversionRules.mysql;

    // Apply conversion rules
    for (const rule of rules) {
      convertedSql = convertedSql.replace(rule.pattern, rule.replacement);
    }

    // Clean up extra whitespace
    convertedSql = convertedSql.replace(/\s+/g, ' ').trim();
    
    // Add Snowflake-specific optimizations
    convertedSql = this.addSnowflakeOptimizations(convertedSql);

    return convertedSql;
  }

  addSnowflakeOptimizations(sql) {
    // Add basic Snowflake optimizations
    let optimizedSql = sql;
    
    // Ensure proper case for Snowflake keywords
    const snowflakeKeywords = [
      'SELECT', 'FROM', 'WHERE', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 'FULL',
      'GROUP BY', 'ORDER BY', 'HAVING', 'INSERT', 'UPDATE', 'DELETE',
      'CREATE', 'ALTER', 'DROP', 'TABLE', 'INDEX', 'VIEW', 'DATABASE',
      'SCHEMA', 'WAREHOUSE', 'STAGE', 'PIPE', 'TASK', 'STREAM'
    ];

    // This is a basic implementation - in production, you'd want more sophisticated parsing
    return optimizedSql;
  }
}

const converter = new SQLConverter();

async function convertToSnowflake(sql, sourceDialect) {
  try {
    return await converter.convertToSnowflake(sql, sourceDialect);
  } catch (error) {
    throw new Error(`Conversion failed: ${error.message}`);
  }
}

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
    // Parse multipart form data
    const result = await multipart.parse(event);
    const files = result.files || [];
    const sourceDialect = result.sourceDialect || 'mysql';

    if (!files || files.length === 0) {
      return {
        statusCode: 400,
        headers,
        body: JSON.stringify({ error: 'No files uploaded' }),
      };
    }

    const processedFiles = await Promise.allSettled(
      files.map(async (file) => {
        const originalSql = file.content.toString('utf-8');
        
        try {
          const convertedSql = await convertToSnowflake(originalSql, sourceDialect);
          return {
            filename: file.filename,
            originalSql,
            convertedSql,
            status: 'success',
            size: file.content.length
          };
        } catch (conversionError) {
          return {
            filename: file.filename,
            originalSql,
            convertedSql: null,
            status: 'error',
            error: conversionError.message,
            size: file.content.length
          };
        }
      })
    );

    const results = processedFiles.map((result, index) => {
      if (result.status === 'fulfilled') {
        return result.value;
      } else {
        return {
          filename: files[index].filename,
          originalSql: files[index].content.toString('utf-8'),
          convertedSql: null,
          status: 'error',
          error: result.reason?.message || 'Conversion failed',
          size: files[index].content.length
        };
      }
    });

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({ files: results }),
    };
  } catch (error) {
    console.error('Conversion error:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ error: 'Internal server error during conversion' }),
    };
  }
};