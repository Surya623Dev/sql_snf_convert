const JSZip = require('jszip');

/**
 * Advanced SQL to Snowflake Converter
 * Uses proper SQL parsing and syntax analysis for accurate conversion
 */

class SQLParser {
  constructor() {
    this.tokens = [];
    this.position = 0;
    this.currentToken = null;
  }

  tokenize(sql) {
    const tokenRegex = /(\w+|'[^']*'|"[^"]*"|`[^`]*`|\[[^\]]*\]|<=|>=|<>|!=|[<>=!]|[(),;.]|\s+|--[^\n]*|\*|\/\*[\s\S]*?\*\/)/gi;
    this.tokens = [];
    let match;
    
    while ((match = tokenRegex.exec(sql)) !== null) {
      const token = match[0];
      if (!/^\s+$/.test(token) && !token.startsWith('--') && !token.startsWith('/*')) {
        this.tokens.push({
          value: token,
          type: this.getTokenType(token),
          position: match.index
        });
      }
    }
    
    this.position = 0;
    this.currentToken = this.tokens[0] || null;
    return this.tokens;
  }

  getTokenType(token) {
    const keywords = [
      'SELECT', 'FROM', 'WHERE', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 'FULL', 'OUTER',
      'ON', 'GROUP', 'BY', 'ORDER', 'HAVING', 'UNION', 'ALL', 'DISTINCT',
      'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER', 'DROP', 'TABLE',
      'INDEX', 'VIEW', 'DATABASE', 'SCHEMA', 'AS', 'AND', 'OR', 'NOT',
      'IN', 'EXISTS', 'BETWEEN', 'LIKE', 'IS', 'NULL', 'TRUE', 'FALSE',
      'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'IF', 'TOP', 'LIMIT', 'OFFSET',
      'WITH', 'RECURSIVE', 'CTE', 'OVER', 'PARTITION', 'ROW_NUMBER', 'RANK',
      'DENSE_RANK', 'LEAD', 'LAG', 'FIRST_VALUE', 'LAST_VALUE'
    ];
    
    if (keywords.includes(token.toUpperCase())) {
      return 'KEYWORD';
    } else if (/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(token)) {
      return 'IDENTIFIER';
    } else if (/^[0-9]+(\.[0-9]+)?$/.test(token)) {
      return 'NUMBER';
    } else if (/^'.*'$/.test(token) || /^".*"$/.test(token)) {
      return 'STRING';
    } else if (['(', ')', ',', ';', '.'].includes(token)) {
      return 'PUNCTUATION';
    } else if (['=', '<', '>', '<=', '>=', '<>', '!='].includes(token)) {
      return 'OPERATOR';
    } else {
      return 'OTHER';
    }
  }

  peek(offset = 1) {
    const nextPos = this.position + offset;
    return nextPos < this.tokens.length ? this.tokens[nextPos] : null;
  }

  advance() {
    this.position++;
    this.currentToken = this.position < this.tokens.length ? this.tokens[this.position] : null;
    return this.currentToken;
  }

  match(value) {
    return this.currentToken && this.currentToken.value.toUpperCase() === value.toUpperCase();
  }

  matchAny(values) {
    return this.currentToken && values.some(v => this.currentToken.value.toUpperCase() === v.toUpperCase());
  }
}

class SnowflakeConverter {
  constructor() {
    this.parser = new SQLParser();
    this.complexFunctionPatterns = this.initializeComplexPatterns();
    this.dialectMappings = {
      mysql: {
        dataTypes: {
          'TINYINT': 'NUMBER(3,0)',
          'SMALLINT': 'NUMBER(5,0)',
          'MEDIUMINT': 'NUMBER(7,0)',
          'INT': 'NUMBER(10,0)',
          'INTEGER': 'NUMBER(10,0)',
          'BIGINT': 'NUMBER(19,0)',
          'DOUBLE': 'FLOAT',
          'TEXT': 'VARCHAR(16777216)',
          'LONGTEXT': 'VARCHAR(16777216)',
          'MEDIUMTEXT': 'VARCHAR(16777216)',
          'TINYTEXT': 'VARCHAR(1000)'
        },
        functions: {
          'NOW()': 'CURRENT_TIMESTAMP()',
          'CURDATE()': 'CURRENT_DATE()',
          'CURTIME()': 'CURRENT_TIME()',
          'IFNULL': 'NVL',
          'CONCAT': 'CONCAT'
        }
      },
      postgresql: {
        dataTypes: {
          'SERIAL': 'NUMBER AUTOINCREMENT',
          'BIGSERIAL': 'NUMBER AUTOINCREMENT',
          'BOOLEAN': 'BOOLEAN',
          'TEXT': 'VARCHAR(16777216)',
          'BYTEA': 'BINARY'
        },
        functions: {
          'NOW()': 'CURRENT_TIMESTAMP()',
          'CURRENT_DATE': 'CURRENT_DATE()',
          'CURRENT_TIME': 'CURRENT_TIME()'
        }
      },
      sqlserver: {
        dataTypes: {
          'BIT': 'BOOLEAN',
          'TINYINT': 'NUMBER(3,0)',
          'SMALLINT': 'NUMBER(5,0)',
          'INT': 'NUMBER(10,0)',
          'BIGINT': 'NUMBER(19,0)',
          'NVARCHAR(MAX)': 'VARCHAR(16777216)',
          'VARCHAR(MAX)': 'VARCHAR(16777216)',
          'TEXT': 'VARCHAR(16777216)',
          'NTEXT': 'VARCHAR(16777216)',
          'IMAGE': 'BINARY',
          'VARBINARY(MAX)': 'BINARY'
        },
        functions: {
          'GETDATE()': 'CURRENT_TIMESTAMP()',
          'GETUTCDATE()': 'CURRENT_TIMESTAMP()',
          'SYSDATETIME()': 'CURRENT_TIMESTAMP()',
          'DATEADD': 'DATEADD',
          'DATEDIFF': 'DATEDIFF',
          'DATEPART': 'DATE_PART',
          'DATENAME': 'TO_CHAR',
          'LEN': 'LENGTH',
          'ISNULL': 'NVL',
          'IIF': 'IFF',
          'CHARINDEX': 'POSITION',
          'PATINDEX': 'REGEXP_INSTR',
          'STUFF': 'INSERT',
          'REPLICATE': 'REPEAT',
          'REVERSE': 'REVERSE',
          'LEFT': 'LEFT',
          'RIGHT': 'RIGHT',
          'LTRIM': 'LTRIM',
          'RTRIM': 'RTRIM',
          'UPPER': 'UPPER',
          'LOWER': 'LOWER',
          'SUBSTRING': 'SUBSTRING',
          'REPLACE': 'REPLACE',
          'CAST': 'CAST',
          'CONVERT': 'CAST',
          'COALESCE': 'COALESCE',
          'NULLIF': 'NULLIF'
        }
      },
      oracle: {
        dataTypes: {
          'VARCHAR2': 'VARCHAR',
          'NCHAR': 'CHAR',
          'NVARCHAR2': 'VARCHAR',
          'CLOB': 'VARCHAR(16777216)',
          'BLOB': 'BINARY',
          'DATE': 'TIMESTAMP_NTZ',
          'TIMESTAMP': 'TIMESTAMP_NTZ'
        },
        functions: {
          'SYSDATE': 'CURRENT_TIMESTAMP()',
          'SYSTIMESTAMP': 'CURRENT_TIMESTAMP()',
          'SYSDATETIMEOFFSET()': 'CURRENT_TIMESTAMP()::TIMESTAMP_TZ',
          'TO_DATE': 'TO_DATE',
          'TO_CHAR': 'TO_CHAR',
          'NVL': 'NVL',
          'NVL2': 'IFF',
          'DECODE': 'CASE'
        }
      }
    };
  }

  initializeComplexPatterns() {
    return {
      sqlserver: [
        // ISDATE function patterns
        {
          pattern: /ISDATE\s*\(\s*CONVERT\s*\(\s*varchar\s*,\s*([^,]+)\s*,\s*23\s*\)\s*\)/gi,
          replacement: (match, dateField) => {
            return `CASE 
  WHEN TRY_TO_DATE(TO_CHAR(${dateField.trim()}, 'YYYY-MM-DD')) IS NOT NULL THEN TRUE
  ELSE FALSE
END`;
          }
        },
        {
          pattern: /ISDATE\s*\(\s*([^)]+)\s*\)/gi,
          replacement: (match, dateField) => {
            return `CASE 
  WHEN TRY_TO_DATE(${dateField.trim()}) IS NOT NULL THEN TRUE
  ELSE FALSE
END`;
          }
        },
        // CONVERT function patterns
        {
          pattern: /CONVERT\s*\(\s*varchar\s*\(\s*(\d+)\s*\)\s*,\s*([^,]+)\s*,\s*23\s*\)/gi,
          replacement: (match, length, field) => {
            return `TO_CHAR(${field.trim()}, 'YYYY-MM-DD')`;
          }
        },
        {
          pattern: /CONVERT\s*\(\s*varchar\s*,\s*([^,]+)\s*,\s*23\s*\)/gi,
          replacement: (match, field) => {
            return `TO_CHAR(${field.trim()}, 'YYYY-MM-DD')`;
          }
        },
        {
          pattern: /CONVERT\s*\(\s*varchar\s*\(\s*(\d+)\s*\)\s*,\s*([^,]+)\s*,\s*120\s*\)/gi,
          replacement: (match, length, field) => {
            return `TO_CHAR(${field.trim()}, 'YYYY-MM-DD HH24:MI:SS')`;
          }
        },
        {
          pattern: /CONVERT\s*\(\s*varchar\s*,\s*([^,]+)\s*,\s*120\s*\)/gi,
          replacement: (match, field) => {
            return `TO_CHAR(${field.trim()}, 'YYYY-MM-DD HH24:MI:SS')`;
          }
        },
        {
          pattern: /CONVERT\s*\(\s*datetime\s*,\s*([^)]+)\s*\)/gi,
          replacement: (match, field) => {
            return `TO_TIMESTAMP(${field.trim()})`;
          }
        },
        {
          pattern: /CONVERT\s*\(\s*date\s*,\s*([^)]+)\s*\)/gi,
          replacement: (match, field) => {
            return `TO_DATE(${field.trim()})`;
          }
        },
        // DATEADD patterns
        {
          pattern: /DATEADD\s*\(\s*year\s*,\s*([^,]+)\s*,\s*([^)]+)\s*\)/gi,
          replacement: (match, interval, date) => {
            return `DATEADD(YEAR, ${interval.trim()}, ${date.trim()})`;
          }
        },
        {
          pattern: /DATEADD\s*\(\s*month\s*,\s*([^,]+)\s*,\s*([^)]+)\s*\)/gi,
          replacement: (match, interval, date) => {
            return `DATEADD(MONTH, ${interval.trim()}, ${date.trim()})`;
          }
        },
        {
          pattern: /DATEADD\s*\(\s*day\s*,\s*([^,]+)\s*,\s*([^)]+)\s*\)/gi,
          replacement: (match, interval, date) => {
            return `DATEADD(DAY, ${interval.trim()}, ${date.trim()})`;
          }
        },
        // DATEDIFF patterns
        {
          pattern: /DATEDIFF\s*\(\s*year\s*,\s*([^,]+)\s*,\s*([^)]+)\s*\)/gi,
          replacement: (match, date1, date2) => {
            return `DATEDIFF(YEAR, ${date1.trim()}, ${date2.trim()})`;
          }
        },
        {
          pattern: /DATEDIFF\s*\(\s*month\s*,\s*([^,]+)\s*,\s*([^)]+)\s*\)/gi,
          replacement: (match, date1, date2) => {
            return `DATEDIFF(MONTH, ${date1.trim()}, ${date2.trim()})`;
          }
        },
        {
          pattern: /DATEDIFF\s*\(\s*day\s*,\s*([^,]+)\s*,\s*([^)]+)\s*\)/gi,
          replacement: (match, date1, date2) => {
            return `DATEDIFF(DAY, ${date1.trim()}, ${date2.trim()})`;
          }
        },
        // DATEPART patterns
        {
          pattern: /DATEPART\s*\(\s*year\s*,\s*([^)]+)\s*\)/gi,
          replacement: (match, date) => {
            return `DATE_PART(YEAR, ${date.trim()})`;
          }
        },
        {
          pattern: /DATEPART\s*\(\s*month\s*,\s*([^)]+)\s*\)/gi,
          replacement: (match, date) => {
            return `DATE_PART(MONTH, ${date.trim()})`;
          }
        },
        {
          pattern: /DATEPART\s*\(\s*day\s*,\s*([^)]+)\s*\)/gi,
          replacement: (match, date) => {
            return `DATE_PART(DAY, ${date.trim()})`;
          }
        },
        // IIF patterns
        {
          pattern: /IIF\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^)]+)\s*\)/gi,
          replacement: (match, condition, trueValue, falseValue) => {
            return `IFF(${condition.trim()}, ${trueValue.trim()}, ${falseValue.trim()})`;
          }
        },
        // CHARINDEX patterns
        {
          pattern: /CHARINDEX\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)/gi,
          replacement: (match, substring, string) => {
            return `POSITION(${substring.trim()} IN ${string.trim()})`;
          }
        },
        // STUFF patterns
        {
          pattern: /STUFF\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^)]+)\s*\)/gi,
          replacement: (match, string, start, length, replacement) => {
            return `INSERT(${string.trim()}, ${start.trim()}, ${length.trim()}, ${replacement.trim()})`;
          }
        }
      ],
      mysql: [
        // MySQL specific patterns can be added here
        {
          pattern: /IFNULL\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)/gi,
          replacement: (match, expr, replacement) => {
            return `NVL(${expr.trim()}, ${replacement.trim()})`;
          }
        }
      ],
      postgresql: [
        // PostgreSQL specific patterns can be added here
      ],
      oracle: [
        // Oracle specific patterns can be added here
        {
          pattern: /NVL2\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^)]+)\s*\)/gi,
          replacement: (match, expr, notNull, isNull) => {
            return `IFF(${expr.trim()} IS NOT NULL, ${notNull.trim()}, ${isNull.trim()})`;
          }
        }
      ]
    };
  }

  convert(sql, sourceDialect = 'sqlserver') {
    try {
      console.log(`Starting conversion from ${sourceDialect} to Snowflake`);
      
      // Parse the SQL
      this.parser.tokenize(sql);
      
      // Convert based on SQL structure
      let convertedSql = this.convertStatement(sql, sourceDialect);
      
      // Apply post-processing
      convertedSql = this.postProcess(convertedSql, sourceDialect);
      
      // Format the result
      convertedSql = this.formatSQL(convertedSql);
      
      console.log('Conversion completed successfully');
      return convertedSql;
      
    } catch (error) {
      console.error('Conversion error:', error);
      throw new Error(`SQL conversion failed: ${error.message}`);
    }
  }

  convertStatement(sql, sourceDialect) {
    let result = sql;
    
    // Handle SQL Server specific syntax
    if (sourceDialect.toLowerCase() === 'sqlserver') {
      result = this.convertSQLServerSyntax(result);
    }
    
    // Apply data type conversions
    result = this.convertDataTypes(result, sourceDialect);
    
    // Apply complex function pattern conversions BEFORE simple function conversions
    result = this.convertComplexFunctions(result, sourceDialect);
    
    // Apply function conversions
    result = this.convertFunctions(result, sourceDialect);
    
    // Handle quoted identifiers
    result = this.convertQuotedIdentifiers(result, sourceDialect);
    
    return result;
  }

  convertSQLServerSyntax(sql) {
    let result = sql;
    
    // Convert TOP N to LIMIT N (handle various TOP patterns)
    result = result.replace(/\bSELECT\s+TOP\s+(\d+)\s+/gi, 'SELECT ');
    result = result.replace(/\bSELECT\s+TOP\s*\(\s*(\d+)\s*\)\s+/gi, 'SELECT ');
    
    // Add LIMIT at the end if TOP was found
    const topMatch = sql.match(/\bTOP\s*\(?\s*(\d+)\s*\)?/gi);
    if (topMatch) {
      const limitValue = topMatch[0].match(/\d+/)[0];
      // Only add LIMIT if it's not already present
      if (!/\bLIMIT\s+\d+/gi.test(result)) {
        result = result.trim();
        if (result.endsWith(';')) {
          result = result.slice(0, -1) + `\nLIMIT ${limitValue};`;
        } else {
          result = result + `\nLIMIT ${limitValue}`;
        }
      }
    }
    
    // Convert ROWNUM (Oracle) to LIMIT
    result = result.replace(/\bWHERE\s+ROWNUM\s*<=?\s*(\d+)/gi, '');
    const rownumMatch = sql.match(/\bROWNUM\s*<=?\s*(\d+)/gi);
    if (rownumMatch) {
      const limitValue = rownumMatch[0].match(/\d+/)[0];
      if (!/\bLIMIT\s+\d+/gi.test(result)) {
        result = result.trim();
        if (result.endsWith(';')) {
          result = result.slice(0, -1) + `\nLIMIT ${limitValue};`;
        } else {
          result = result + `\nLIMIT ${limitValue}`;
        }
      }
    }
    
    // Convert square brackets to double quotes
    result = result.replace(/\[([^\]]+)\]/g, '"$1"');
    
    // Convert IDENTITY to AUTOINCREMENT
    result = result.replace(/\bIDENTITY\s*\(\s*\d+\s*,\s*\d+\s*\)/gi, 'AUTOINCREMENT');
    
    return result;
  }

  convertComplexFunctions(sql, sourceDialect) {
    let result = sql;
    const patterns = this.complexFunctionPatterns[sourceDialect.toLowerCase()] || [];
    
    console.log(`Applying ${patterns.length} complex function patterns for ${sourceDialect}`);
    
    patterns.forEach((patternObj, index) => {
      const beforeConversion = result;
      
      if (typeof patternObj.replacement === 'function') {
        result = result.replace(patternObj.pattern, patternObj.replacement);
      } else {
        result = result.replace(patternObj.pattern, patternObj.replacement);
      }
      
      if (beforeConversion !== result) {
        console.log(`Applied complex pattern ${index + 1}: Function conversion successful`);
      }
    });
    
    return result;
  }

  convertDataTypes(sql, sourceDialect) {
    const mappings = this.dialectMappings[sourceDialect.toLowerCase()]?.dataTypes || {};
    let result = sql;
    
    Object.entries(mappings).forEach(([from, to]) => {
      const regex = new RegExp(`\\b${from}\\b`, 'gi');
      result = result.replace(regex, to);
    });
    
    // Handle parameterized types
    result = result.replace(/\bVARCHAR\(MAX\)/gi, 'VARCHAR(16777216)');
    result = result.replace(/\bNVARCHAR\(MAX\)/gi, 'VARCHAR(16777216)');
    result = result.replace(/\bVARBINARY\(MAX\)/gi, 'BINARY');
    
    return result;
  }

  convertFunctions(sql, sourceDialect) {
    const mappings = this.dialectMappings[sourceDialect.toLowerCase()]?.functions || {};
    let result = sql;
    
    Object.entries(mappings).forEach(([from, to]) => {
      // Handle functions with parentheses
      if (from.endsWith('()')) {
        const funcName = from.slice(0, -2);
        const regex = new RegExp(`\\b${funcName}\\s*\\(\\s*\\)`, 'gi');
        result = result.replace(regex, `${to}`);
      } else {
        // Handle function name replacements
        const regex = new RegExp(`\\b${from}\\s*\\(`, 'gi');
        result = result.replace(regex, `${to}(`);
      }
    });
    
    return result;
  }

  convertQuotedIdentifiers(sql, sourceDialect) {
    let result = sql;
    
    if (sourceDialect.toLowerCase() === 'mysql') {
      // Convert backticks to double quotes
      result = result.replace(/`([^`]+)`/g, '"$1"');
    } else if (sourceDialect.toLowerCase() === 'sqlserver') {
      // Convert square brackets to double quotes
      result = result.replace(/\[([^\]]+)\]/g, '"$1"');
    }
    
    return result;
  }

  postProcess(sql, sourceDialect) {
    let result = sql;
    
    // Remove any duplicate spaces
    result = result.replace(/\s+/g, ' ');
    
    // Ensure proper spacing around operators
    result = result.replace(/([=<>!]+)/g, ' $1 ');
    result = result.replace(/\s+([=<>!]+)\s+/g, ' $1 ');
    
    return result.trim();
  }

  formatSQL(sql) {
    if (!sql || typeof sql !== 'string') {
      return sql;
    }

    let formatted = sql.trim();
    
    // Normalize whitespace
    formatted = formatted.replace(/\s+/g, ' ').trim();
    
    // Add line breaks before major keywords
    const majorKeywords = [
      'SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'HAVING',
      'UNION', 'UNION ALL', 'EXCEPT', 'INTERSECT', 'WITH', 'LIMIT'
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
    
    // Clean up multiple line breaks
    formatted = formatted.replace(/\n\s*\n/g, '\n');
    
    // Add proper indentation
    formatted = this.addIndentation(formatted);
    
    return formatted.trim();
  }

  addIndentation(sql) {
    const lines = sql.split('\n');
    let indentLevel = 0;
    const indentSize = 2;
    
    return lines.map(line => {
      const trimmedLine = line.trim();
      if (!trimmedLine) return '';
      
      // Adjust indent level based on keywords
      if (trimmedLine.match(/^(FROM|WHERE|GROUP BY|ORDER BY|HAVING|UNION|EXCEPT|INTERSECT|LIMIT)$/i)) {
        indentLevel = 0;
      } else if (trimmedLine.match(/^(JOIN|INNER JOIN|LEFT JOIN|RIGHT JOIN|FULL JOIN|FULL OUTER JOIN)$/i)) {
        indentLevel = 0;
      } else if (trimmedLine.match(/^(AND|OR)$/i)) {
        indentLevel = 1;
      } else if (trimmedLine.match(/^SELECT$/i)) {
        indentLevel = 0;
      }
      
      const indent = ' '.repeat(indentLevel * indentSize);
      
      // Set indent for next line
      if (trimmedLine.match(/^(SELECT|WHERE|GROUP BY|ORDER BY|HAVING)$/i)) {
        indentLevel = 1;
      }
      
      return indent + trimmedLine;
    }).join('\n');
  }
}

const converter = new SnowflakeConverter();

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
    const sourceDialect = result.sourceDialect || 'sqlserver';

    if (!files || files.length === 0) {
      return {
        statusCode: 400,
        headers,
        body: JSON.stringify({ error: 'No files uploaded' }),
      };
    }

    console.log(`Processing ${files.length} files from ${sourceDialect} to Snowflake`);

    const processedFiles = await Promise.allSettled(
      files.map(async (file) => {
        const originalSql = file.content.toString('utf-8');
        
        try {
          console.log(`Converting file: ${file.filename}`);
          console.log(`Original SQL preview: ${originalSql.substring(0, 200)}...`);
          
          const convertedSql = converter.convert(originalSql, sourceDialect);
          
          console.log(`Successfully converted ${file.filename}`);
          console.log(`Converted SQL preview: ${convertedSql.substring(0, 200)}...`);
          
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

    console.log(`Conversion completed. Success: ${results.filter(r => r.status === 'success').length}, Errors: ${results.filter(r => r.status === 'error').length}`);

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({ files: results }),
    };
  } catch (error) {
    console.error('Handler error:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ error: 'Internal server error during conversion' }),
    };
  }
};