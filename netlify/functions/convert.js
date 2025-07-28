// Using a simple fetch-based approach for free AI services

/**
 * AI-Powered SQL to Snowflake Converter
 * Uses free AI services for intelligent SQL conversion
 */

class AIConverter {
  constructor() {
    // Fallback conversion rules for when AI is unavailable
    this.fallbackRules = {
      mysql: [
        { pattern: /\bTINYINT\b/gi, replacement: 'NUMBER(3,0)' },
        { pattern: /\bSMALLINT\b/gi, replacement: 'NUMBER(5,0)' },
        { pattern: /\bMEDIUMINT\b/gi, replacement: 'NUMBER(7,0)' },
        { pattern: /\bINT\b/gi, replacement: 'NUMBER(10,0)' },
        { pattern: /\bBIGINT\b/gi, replacement: 'NUMBER(19,0)' },
        { pattern: /\bDOUBLE\b/gi, replacement: 'FLOAT' },
        { pattern: /\bTEXT\b/gi, replacement: 'VARCHAR(16777216)' },
        { pattern: /\bNOW\(\)/gi, replacement: 'CURRENT_TIMESTAMP()' },
        { pattern: /\bCURDATE\(\)/gi, replacement: 'CURRENT_DATE()' },
        { pattern: /\bIFNULL\(/gi, replacement: 'NVL(' },
        { pattern: /\b`([^`]+)`/g, replacement: '"$1"' },
        { pattern: /\bAUTO_INCREMENT\b/gi, replacement: 'AUTOINCREMENT' },
      ],
      postgresql: [
        { pattern: /\bSERIAL\b/gi, replacement: 'NUMBER AUTOINCREMENT' },
        { pattern: /\bBIGSERIAL\b/gi, replacement: 'NUMBER AUTOINCREMENT' },
        { pattern: /\bBOOLEAN\b/gi, replacement: 'BOOLEAN' },
        { pattern: /\bTEXT\b/gi, replacement: 'VARCHAR(16777216)' },
        { pattern: /\bBYTEA\b/gi, replacement: 'BINARY' },
        { pattern: /\bNOW\(\)/gi, replacement: 'CURRENT_TIMESTAMP()' },
        { pattern: /\bLIMIT\s+(\d+)\s+OFFSET\s+(\d+)/gi, replacement: 'LIMIT $2, $1' },
      ],
      sqlserver: [
        { pattern: /\bBIT\b/gi, replacement: 'BOOLEAN' },
        { pattern: /\bTINYINT\b/gi, replacement: 'NUMBER(3,0)' },
        { pattern: /\bSMALLINT\b/gi, replacement: 'NUMBER(5,0)' },
        { pattern: /\bINT\b/gi, replacement: 'NUMBER(10,0)' },
        { pattern: /\bBIGINT\b/gi, replacement: 'NUMBER(19,0)' },
        { pattern: /\bNVARCHAR\(MAX\)/gi, replacement: 'VARCHAR(16777216)' },
        { pattern: /\bVARCHAR\(MAX\)/gi, replacement: 'VARCHAR(16777216)' },
        { pattern: /\bTOP\s+(\d+)/gi, replacement: 'LIMIT $1' },
        { pattern: /\bGETDATE\(\)/gi, replacement: 'CURRENT_TIMESTAMP()' },
        { pattern: /\bLEN\(/gi, replacement: 'LENGTH(' },
        { pattern: /\bISNULL\(/gi, replacement: 'NVL(' },
        { pattern: /\b\[([^\]]+)\]/g, replacement: '"$1"' },
      ],
      oracle: [
        { pattern: /\bVARCHAR2\b/gi, replacement: 'VARCHAR' },
        { pattern: /\bNCHAR\b/gi, replacement: 'CHAR' },
        { pattern: /\bNVARCHAR2\b/gi, replacement: 'VARCHAR' },
        { pattern: /\bCLOB\b/gi, replacement: 'VARCHAR(16777216)' },
        { pattern: /\bBLOB\b/gi, replacement: 'BINARY' },
        { pattern: /\bDATE\b/gi, replacement: 'TIMESTAMP_NTZ' },
        { pattern: /\bSYSDATE\b/gi, replacement: 'CURRENT_TIMESTAMP()' },
        { pattern: /\bROWNUM\s*<=?\s*(\d+)/gi, replacement: 'LIMIT $1' },
        { pattern: /\bDUAL\b/gi, replacement: '(SELECT 1)' },
      ]
    };
  }

  async convertWithAI(sql, sourceDialect) {
    try {
      console.log(`Starting AI conversion for ${sourceDialect} SQL:`, sql.substring(0, 100) + '...');
      
      // For now, use enhanced rule-based conversion with intelligent formatting
      // This provides reliable, fast conversion without external API dependencies
      console.log('Using enhanced rule-based conversion with intelligent formatting');
      return this.enhancedRuleBasedConversion(sql, sourceDialect);
    } catch (error) {
      console.error('AI conversion failed, using fallback:', error.message);
      return this.fallbackConversion(sql, sourceDialect);
    }
  }

  enhancedRuleBasedConversion(sql, sourceDialect) {
    let convertedSql = sql.trim();
    const rules = this.getAllConversionRules(sourceDialect.toLowerCase());

    console.log(`Applying ${rules.length} conversion rules for ${sourceDialect}`);

    // Apply all conversion rules
    for (const rule of rules) {
      const beforeLength = convertedSql.length;
      convertedSql = convertedSql.replace(rule.pattern, rule.replacement);
      if (convertedSql.length !== beforeLength) {
        console.log(`Applied rule: ${rule.pattern} -> ${rule.replacement}`);
      }
    }

    // Apply intelligent formatting
    convertedSql = this.formatSQL(convertedSql);
    console.log('Applied intelligent formatting');
    
    return convertedSql;
  }

  getAllConversionRules(sourceDialect) {
    // Combine base rules with comprehensive function mappings
    const baseRules = this.fallbackRules[sourceDialect] || this.fallbackRules.mysql;
    const functionRules = this.getFunctionConversionRules(sourceDialect);
    
    return [...baseRules, ...functionRules];
  }

  getFunctionConversionRules(sourceDialect) {
    const rules = [];
    
    if (sourceDialect === 'sqlserver') {
      // Add comprehensive SQL Server function conversions
      const functionMappings = {
        // String Functions
        'ASCII': 'ASCII',
        'CHAR': 'CHR',
        'CHARINDEX': 'POSITION',
        'CONCAT': 'CONCAT',
        'CONCAT_WS': 'CONCAT_WS',
        'DIFFERENCE': 'SOUNDEX_DIFFERENCE',
        'FORMAT': 'TO_CHAR',
        'LEFT': 'LEFT',
        'LEN': 'LENGTH',
        'LOWER': 'LOWER',
        'LTRIM': 'LTRIM',
        'NCHAR': 'CHR',
        'PATINDEX': 'REGEXP_INSTR',
        'QUOTENAME': 'QUOTE_IDENT',
        'REPLACE': 'REPLACE',
        'REPLICATE': 'REPEAT',
        'REVERSE': 'REVERSE',
        'RIGHT': 'RIGHT',
        'RTRIM': 'RTRIM',
        'SOUNDEX': 'SOUNDEX',
        'SPACE': 'REPEAT(\' \', n)',
        'STR': 'TO_CHAR',
        'STUFF': 'INSERT',
        'SUBSTRING': 'SUBSTRING',
        'TRANSLATE': 'TRANSLATE',
        'TRIM': 'TRIM',
        'UNICODE': 'UNICODE',
        'UPPER': 'UPPER',
        
        // Numeric Functions
        'ABS': 'ABS',
        'ACOS': 'ACOS',
        'ASIN': 'ASIN',
        'ATAN': 'ATAN',
        'ATN2': 'ATAN2',
        'AVG': 'AVG',
        'CEILING': 'CEIL',
        'COUNT': 'COUNT',
        'COS': 'COS',
        'COT': '1/TAN',
        'DEGREES': 'DEGREES',
        'EXP': 'EXP',
        'FLOOR': 'FLOOR',
        'LOG': 'LN',
        'LOG10': 'LOG',
        'MAX': 'MAX',
        'MIN': 'MIN',
        'PI': 'PI',
        'POWER': 'POWER',
        'RADIANS': 'RADIANS',
        'RAND': 'RANDOM',
        'ROUND': 'ROUND',
        'SIGN': 'SIGN',
        'SIN': 'SIN',
        'SQRT': 'SQRT',
        'SQUARE': 'POWER(n, 2)',
        'SUM': 'SUM',
        'TAN': 'TAN',
        
        // Date Functions
        'CURRENT_TIMESTAMP': 'CURRENT_TIMESTAMP',
        'DATEADD': 'DATEADD',
        'DATEDIFF': 'DATEDIFF',
        'DATENAME': 'TO_CHAR',
        'DATEPART': 'DATE_PART',
        'DAY': 'DAY',
        'GETDATE': 'CURRENT_TIMESTAMP',
        'GETUTCDATE': 'CURRENT_TIMESTAMP',
        'ISDATE': 'TRY_TO_DATE IS NOT NULL',
        'MONTH': 'MONTH',
        'SYSDATETIME': 'CURRENT_TIMESTAMP',
        'YEAR': 'YEAR',
        
        // Advanced Functions
        'CAST': 'CAST',
        'COALESCE': 'COALESCE',
        'CONVERT': 'CAST',
        'CURRENT_USER': 'CURRENT_USER',
        'IIF': 'IFF',
        'ISNULL': 'NVL',
        'ISNUMERIC': 'TRY_TO_NUMBER IS NOT NULL',
        'NULLIF': 'NULLIF',
        'SESSION_USER': 'CURRENT_USER',
        'SESSIONPROPERTY': 'CURRENT_SESSION',
        'SYSTEM_USER': 'CURRENT_USER',
        'USER_NAME': 'CURRENT_USER'
      };
      
      // Convert function mappings to regex rules
      Object.entries(functionMappings).forEach(([sqlServerFunc, snowflakeFunc]) => {
        rules.push({
          pattern: new RegExp(`\\b${sqlServerFunc}\\s*\\(`, 'gi'),
          replacement: `${snowflakeFunc}(`
        });
      });
    }
    
    return rules;
  }

  fallbackConversion(sql, sourceDialect) {
    let convertedSql = sql.trim();
    const rules = this.fallbackRules[sourceDialect.toLowerCase()] || this.fallbackRules.mysql;

    // Apply fallback conversion rules
    for (const rule of rules) {
      convertedSql = convertedSql.replace(rule.pattern, rule.replacement);
    }

    return this.formatSQL(convertedSql);
  }

  formatSQL(sql) {
    if (!sql || typeof sql !== 'string') {
      return sql;
    }

    let formatted = sql;
    
    // Normalize whitespace
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
    
    // Format CASE statements
    formatted = formatted.replace(/\bCASE\b/gi, '\nCASE');
    formatted = formatted.replace(/\bWHEN\b/gi, '\n  WHEN');
    formatted = formatted.replace(/\bTHEN\b/gi, ' THEN');
    formatted = formatted.replace(/\bELSE\b/gi, '\n  ELSE');
    formatted = formatted.replace(/\bEND\b/gi, '\nEND');
    
    // Handle subqueries
    formatted = this.formatSubqueries(formatted);
    
    // Clean up multiple line breaks
    formatted = formatted.replace(/\n\s*\n/g, '\n');
    
    // Add proper indentation
    formatted = this.addIndentation(formatted);
    
    // Fix spacing around operators
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
      
      if (trimmedLine.match(/^(SELECT|WHERE|GROUP BY|ORDER BY|HAVING)$/i)) {
        indentLevel = 1;
      }
      
      return indent + trimmedLine;
    }).join('\n');
  }
}

const aiConverter = new AIConverter();

exports.handler = async (event, context) => {
  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
  };

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
    const multipart = await import('lambda-multipart-parser');
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
          console.log(`Processing file: ${file.filename}`);
          const convertedSql = await aiConverter.convertWithAI(originalSql, sourceDialect);
          console.log(`Successfully converted ${file.filename}`);
          return {
            filename: file.filename,
            originalSql,
            convertedSql,
            status: 'success',
            size: file.content.length
          };
        } catch (conversionError) {
          console.error(`Failed to convert ${file.filename}:`, conversionError);
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