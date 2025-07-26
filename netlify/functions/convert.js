const multipart = require('lambda-multipart-parser');

/**
 * SQL to Snowflake Converter
 * Handles conversion from various SQL dialects to Snowflake-compatible SQL
 */

class SQLConverter {
  constructor() {
    this.keywords = [
      'SELECT', 'FROM', 'WHERE', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 'FULL', 'OUTER',
      'GROUP BY', 'ORDER BY', 'HAVING', 'INSERT', 'UPDATE', 'DELETE', 'CREATE',
      'ALTER', 'DROP', 'TABLE', 'INDEX', 'VIEW', 'DATABASE', 'SCHEMA', 'UNION',
      'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'AND', 'OR', 'NOT', 'IN', 'EXISTS',
      'BETWEEN', 'LIKE', 'IS', 'NULL', 'DISTINCT', 'AS', 'ON', 'USING', 'LIMIT',
      'OFFSET', 'WITH', 'CTE', 'RECURSIVE', 'PARTITION', 'OVER', 'WINDOW'
    ];
    
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
        
        // String Functions
        { pattern: /\bASCII\(/gi, replacement: 'ASCII(' },
        { pattern: /\bCHAR\(/gi, replacement: 'CHR(' },
        { pattern: /\bCHARINDEX\(/gi, replacement: 'POSITION(' },
        { pattern: /\bCONCAT\(/gi, replacement: 'CONCAT(' },
        { pattern: /\bCONCAT_WS\(/gi, replacement: 'CONCAT_WS(' },
        { pattern: /\bDIFFERENCE\(/gi, replacement: 'SOUNDEX_DIFFERENCE(' },
        { pattern: /\bFORMAT\(/gi, replacement: 'TO_CHAR(' },
        { pattern: /\bLEFT\(/gi, replacement: 'LEFT(' },
        { pattern: /\bLEN\(/gi, replacement: 'LENGTH(' },
        { pattern: /\bLOWER\(/gi, replacement: 'LOWER(' },
        { pattern: /\bLTRIM\(/gi, replacement: 'LTRIM(' },
        { pattern: /\bNCHAR\(/gi, replacement: 'CHR(' },
        { pattern: /\bPATINDEX\(/gi, replacement: 'REGEXP_INSTR(' },
        { pattern: /\bQUOTENAME\(/gi, replacement: 'QUOTE_IDENT(' },
        { pattern: /\bREPLACE\(/gi, replacement: 'REPLACE(' },
        { pattern: /\bREPLICATE\(/gi, replacement: 'REPEAT(' },
        { pattern: /\bREVERSE\(/gi, replacement: 'REVERSE(' },
        { pattern: /\bRIGHT\(/gi, replacement: 'RIGHT(' },
        { pattern: /\bRTRIM\(/gi, replacement: 'RTRIM(' },
        { pattern: /\bSOUNDEX\(/gi, replacement: 'SOUNDEX(' },
        { pattern: /\bSPACE\(/gi, replacement: 'REPEAT(\' \', ' },
        { pattern: /\bSTR\(/gi, replacement: 'TO_CHAR(' },
        { pattern: /\bSTUFF\(/gi, replacement: 'INSERT(' },
        { pattern: /\bSUBSTRING\(/gi, replacement: 'SUBSTR(' },
        { pattern: /\bTRANSLATE\(/gi, replacement: 'TRANSLATE(' },
        { pattern: /\bTRIM\(/gi, replacement: 'TRIM(' },
        { pattern: /\bUNICODE\(/gi, replacement: 'UNICODE(' },
        { pattern: /\bUPPER\(/gi, replacement: 'UPPER(' },
        
        // Numeric Functions
        { pattern: /\bABS\(/gi, replacement: 'ABS(' },
        { pattern: /\bACOS\(/gi, replacement: 'ACOS(' },
        { pattern: /\bASIN\(/gi, replacement: 'ASIN(' },
        { pattern: /\bATAN\(/gi, replacement: 'ATAN(' },
        { pattern: /\bATN2\(/gi, replacement: 'ATAN2(' },
        { pattern: /\bAVG\(/gi, replacement: 'AVG(' },
        { pattern: /\bCEILING\(/gi, replacement: 'CEIL(' },
        { pattern: /\bCOUNT\(/gi, replacement: 'COUNT(' },
        { pattern: /\bCOS\(/gi, replacement: 'COS(' },
        { pattern: /\bCOT\(/gi, replacement: 'COT(' },
        { pattern: /\bDEGREES\(/gi, replacement: 'DEGREES(' },
        { pattern: /\bEXP\(/gi, replacement: 'EXP(' },
        { pattern: /\bFLOOR\(/gi, replacement: 'FLOOR(' },
        { pattern: /\bLOG\(/gi, replacement: 'LN(' },
        { pattern: /\bLOG10\(/gi, replacement: 'LOG(' },
        { pattern: /\bMAX\(/gi, replacement: 'MAX(' },
        { pattern: /\bMIN\(/gi, replacement: 'MIN(' },
        { pattern: /\bPI\(\)/gi, replacement: 'PI()' },
        { pattern: /\bPOWER\(/gi, replacement: 'POWER(' },
        { pattern: /\bRADIANS\(/gi, replacement: 'RADIANS(' },
        { pattern: /\bRAND\(/gi, replacement: 'RANDOM(' },
        { pattern: /\bROUND\(/gi, replacement: 'ROUND(' },
        { pattern: /\bSIGN\(/gi, replacement: 'SIGN(' },
        { pattern: /\bSIN\(/gi, replacement: 'SIN(' },
        { pattern: /\bSQRT\(/gi, replacement: 'SQRT(' },
        { pattern: /\bSQUARE\(/gi, replacement: 'SQUARE(' },
        { pattern: /\bSUM\(/gi, replacement: 'SUM(' },
        { pattern: /\bTAN\(/gi, replacement: 'TAN(' },
        
        // Date Functions
        { pattern: /\bCURRENT_TIMESTAMP\b/gi, replacement: 'CURRENT_TIMESTAMP()' },
        { pattern: /\bDATEADD\(/gi, replacement: 'DATEADD(' },
        { pattern: /\bDATEDIFF\(/gi, replacement: 'DATEDIFF(' },
        { pattern: /\bDATEFROMPARTS\(/gi, replacement: 'DATE_FROM_PARTS(' },
        { pattern: /\bDATENAME\(/gi, replacement: 'DAYNAME(' },
        { pattern: /\bDATEPART\(/gi, replacement: 'DATE_PART(' },
        { pattern: /\bDAY\(/gi, replacement: 'DAY(' },
        { pattern: /\bGETDATE\(\)/gi, replacement: 'CURRENT_TIMESTAMP()' },
        { pattern: /\bGETUTCDATE\(\)/gi, replacement: 'CURRENT_TIMESTAMP()' },
        { pattern: /\bISDATE\(/gi, replacement: 'TRY_TO_DATE(' },
        { pattern: /\bMONTH\(/gi, replacement: 'MONTH(' },
        { pattern: /\bSYSDATETIME\(\)/gi, replacement: 'CURRENT_TIMESTAMP()' },
        { pattern: /\bSYSDATETIMEOFFSET\(\)/gi, replacement: 'CURRENT_TIMESTAMP()' },
        { pattern: /\bSYSUTCDATETIME\(\)/gi, replacement: 'CURRENT_TIMESTAMP()' },
        { pattern: /\bYEAR\(/gi, replacement: 'YEAR(' },
        
        // Advanced Functions
        { pattern: /\bCAST\(/gi, replacement: 'CAST(' },
        { pattern: /\bCOALESCE\(/gi, replacement: 'COALESCE(' },
        { pattern: /\bCONVERT\(/gi, replacement: 'TO_VARCHAR(' },
        { pattern: /\bCURRENT_USER\b/gi, replacement: 'CURRENT_USER()' },
        { pattern: /\bIIF\(/gi, replacement: 'IFF(' },
        { pattern: /\bISNULL\(/gi, replacement: 'NVL(' },
        { pattern: /\bISNUMERIC\(/gi, replacement: 'IS_NUMERIC(' },
        { pattern: /\bNULLIF\(/gi, replacement: 'NULLIF(' },
        { pattern: /\bSESSION_USER\b/gi, replacement: 'CURRENT_USER()' },
        { pattern: /\bSYSTEM_USER\b/gi, replacement: 'CURRENT_USER()' },
        { pattern: /\bUSER_NAME\(\)/gi, replacement: 'CURRENT_USER()' },
        
        // Window Functions
        { pattern: /\bROW_NUMBER\(\)/gi, replacement: 'ROW_NUMBER()' },
        { pattern: /\bRANK\(\)/gi, replacement: 'RANK()' },
        { pattern: /\bDENSE_RANK\(\)/gi, replacement: 'DENSE_RANK()' },
        { pattern: /\bNTILE\(/gi, replacement: 'NTILE(' },
        { pattern: /\bLAG\(/gi, replacement: 'LAG(' },
        { pattern: /\bLEAD\(/gi, replacement: 'LEAD(' },
        { pattern: /\bFIRST_VALUE\(/gi, replacement: 'FIRST_VALUE(' },
        { pattern: /\bLAST_VALUE\(/gi, replacement: 'LAST_VALUE(' },
        
        // Aggregate Functions
        { pattern: /\bCHECKSUM_AGG\(/gi, replacement: 'HASH_AGG(' },
        { pattern: /\bCOUNT_BIG\(/gi, replacement: 'COUNT(' },
        { pattern: /\bGROUPING\(/gi, replacement: 'GROUPING(' },
        { pattern: /\bGROUPING_ID\(/gi, replacement: 'GROUPING_ID(' },
        { pattern: /\bSTDEV\(/gi, replacement: 'STDDEV(' },
        { pattern: /\bSTDEVP\(/gi, replacement: 'STDDEV_POP(' },
        { pattern: /\bVAR\(/gi, replacement: 'VARIANCE(' },
        { pattern: /\bVARP\(/gi, replacement: 'VAR_POP(' },
        
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

    // Format the SQL properly
    convertedSql = this.formatSQL(convertedSql);
    
    // Add Snowflake-specific optimizations
    convertedSql = this.addSnowflakeOptimizations(convertedSql);

    return convertedSql;
  }

  formatSQL(sql) {
    if (!sql || typeof sql !== 'string') {
      return sql;
    }

    let formatted = sql;
    
    // Normalize whitespace first
    formatted = formatted.replace(/\s+/g, ' ').trim();
    
    // Add line breaks before major keywords
    const majorKeywords = [
      'SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'HAVING',
      'UNION', 'UNION ALL', 'EXCEPT', 'INTERSECT', 'WITH'
    ];
    
    majorKeywords.forEach(keyword => {
      const regex = new RegExp(`\\b${keyword}\\b`, 'gi');
      formatted = formatted.replace(regex, `\n${keyword}`);
    });
    
    // Add line breaks before JOIN keywords
    const joinKeywords = ['JOIN', 'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'FULL JOIN', 'FULL OUTER JOIN'];
    joinKeywords.forEach(keyword => {
      const regex = new RegExp(`\\b${keyword}\\b`, 'gi');
      formatted = formatted.replace(regex, `\n${keyword}`);
    });
    
    // Handle CASE statements
    formatted = formatted.replace(/\bCASE\b/gi, '\nCASE');
    formatted = formatted.replace(/\bWHEN\b/gi, '\n  WHEN');
    formatted = formatted.replace(/\bTHEN\b/gi, ' THEN');
    formatted = formatted.replace(/\bELSE\b/gi, '\n  ELSE');
    formatted = formatted.replace(/\bEND\b/gi, '\nEND');
    
    // Handle subqueries - add indentation
    formatted = this.formatSubqueries(formatted);
    
    // Clean up multiple line breaks
    formatted = formatted.replace(/\n\s*\n/g, '\n');
    
    // Add proper indentation
    formatted = this.addIndentation(formatted);
    
    // Ensure proper spacing around operators
    formatted = formatted.replace(/([=<>!]+)/g, ' $1 ');
    formatted = formatted.replace(/\s+([=<>!]+)\s+/g, ' $1 ');
    
    // Clean up extra spaces
    formatted = formatted.replace(/ +/g, ' ');
    
    return formatted.trim();
  }
  
  formatSubqueries(sql) {
    let formatted = sql;
    let depth = 0;
    let result = '';
    let inString = false;
    let stringChar = '';
    
    for (let i = 0; i < formatted.length; i++) {
      const char = formatted[i];
      const prevChar = i > 0 ? formatted[i - 1] : '';
      
      // Handle string literals
      if ((char === '"' || char === "'") && prevChar !== '\\') {
        if (!inString) {
          inString = true;
          stringChar = char;
        } else if (char === stringChar) {
          inString = false;
          stringChar = '';
        }
      }
      
      if (!inString) {
        if (char === '(') {
          // Check if this is a subquery
          const nextPart = formatted.substring(i + 1, i + 20).trim().toUpperCase();
          if (nextPart.startsWith('SELECT') || nextPart.startsWith('WITH')) {
            depth++;
            result += char + '\n' + '  '.repeat(depth);
          } else {
            result += char;
          }
        } else if (char === ')' && depth > 0) {
          depth--;
          result += '\n' + '  '.repeat(depth) + char;
        } else {
          result += char;
        }
      } else {
        result += char;
      }
    }
    
    return result;
  }
  
  addIndentation(sql) {
    const lines = sql.split('\n');
    let indentLevel = 0;
    const indentSize = 2;
    
    return lines.map(line => {
      const trimmedLine = line.trim();
      if (!trimmedLine) return '';
      
      // Decrease indent for certain keywords
      if (trimmedLine.match(/^(FROM|WHERE|GROUP BY|ORDER BY|HAVING|UNION|EXCEPT|INTERSECT)$/i)) {
        indentLevel = 0;
      } else if (trimmedLine.match(/^(JOIN|INNER JOIN|LEFT JOIN|RIGHT JOIN|FULL JOIN|FULL OUTER JOIN)$/i)) {
        indentLevel = 0;
      } else if (trimmedLine.match(/^(AND|OR)$/i)) {
        indentLevel = 1;
      } else if (trimmedLine.match(/^SELECT$/i)) {
        indentLevel = 0;
      }
      
      const indent = ' '.repeat(indentLevel * indentSize);
      
      // Increase indent for certain contexts
      if (trimmedLine.match(/^(SELECT|WHERE|GROUP BY|ORDER BY|HAVING)$/i)) {
        indentLevel = 1;
      }
      
      return indent + trimmedLine;
    }).join('\n');
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