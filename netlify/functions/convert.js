const { HfInference } = require('@huggingface/inference');

/**
 * AI-Powered SQL to Snowflake Converter
 * Uses Hugging Face's free inference API for intelligent SQL conversion
 */

class AIConverter {
  constructor() {
    // Initialize Hugging Face client
    this.hf = new HfInference(process.env.HUGGINGFACE_API_KEY || '');
    
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
      
      const prompt = this.buildConversionPrompt(sql, sourceDialect);
      console.log('Generated prompt:', prompt.substring(0, 200) + '...');
      
      // Try multiple models in order of preference
      const models = [
        'microsoft/DialoGPT-medium',
        'gpt2',
        'distilgpt2'
      ];
      
      let response = null;
      let lastError = null;
      
      for (const model of models) {
        try {
          console.log(`Trying model: ${model}`);
          response = await this.hf.textGeneration({
            model: model,
            inputs: prompt,
            parameters: {
              max_new_tokens: 500,
              temperature: 0.3,
              do_sample: true,
              return_full_text: false,
              pad_token_id: 50256
            }
          });
          console.log(`Successfully got response from ${model}`);
          break;
        } catch (modelError) {
          console.log(`Model ${model} failed:`, modelError.message);
          lastError = modelError;
          continue;
        }
      }
      
      if (!response) {
        throw lastError || new Error('All models failed');
      }

      console.log('AI response received:', response);

      let convertedSql = response.generated_text?.trim();
      
      if (!convertedSql || convertedSql.length < 10) {
        console.log('AI response too short, using fallback');
        throw new Error('AI response too short or empty');
      }

      console.log('Raw AI response:', convertedSql);

      // Clean up the AI response
      convertedSql = this.cleanAIResponse(convertedSql);
      console.log('Cleaned AI response:', convertedSql);
      
      // Format the SQL
      convertedSql = this.formatSQL(convertedSql);
      console.log('Formatted SQL:', convertedSql);
      
      return convertedSql;
    } catch (error) {
      console.error('AI conversion failed, using fallback:', error.message);
      return this.fallbackConversion(sql, sourceDialect);
    }
  }

  buildConversionPrompt(sql, sourceDialect) {
    return `Convert ${sourceDialect.toUpperCase()} SQL to Snowflake SQL.

Rules:
- TINYINT/SMALLINT/INT/BIGINT → NUMBER
- VARCHAR(MAX)/TEXT → VARCHAR(16777216)
- GETDATE() → CURRENT_TIMESTAMP()
- TOP N → LIMIT N
- LEN() → LENGTH()
- ISNULL() → NVL()

Input SQL:
${sql}

Snowflake SQL:`;
  }

  cleanAIResponse(response) {
    // Remove common AI response artifacts
    let cleaned = response
      .replace(/^(Snowflake SQL:|Here's the converted SQL:|```sql|```|sql)/i, '')
      .replace(/(```|Here's|The converted|Note:|Explanation:).*$/is, '')
      .trim();

    // Remove any explanatory text after the SQL
    const lines = cleaned.split('\n');
    let sqlLines = [];
    let foundSQL = false;

    for (const line of lines) {
      const trimmedLine = line.trim();
      
      // Skip empty lines at the start
      if (!foundSQL && !trimmedLine) continue;
      
      // Check if this looks like SQL
      if (!foundSQL && (
        /^(SELECT|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|WITH|MERGE)/i.test(trimmedLine) ||
        /^(FROM|WHERE|JOIN|GROUP BY|ORDER BY|HAVING|UNION)/i.test(trimmedLine) ||
        /^\s*(--|\/\*)/i.test(trimmedLine) // SQL comments
      )) {
        foundSQL = true;
      }
      
      // Stop if we hit explanatory text after SQL
      if (foundSQL && (
        /^(This|The|Note:|Explanation:|Here|Key changes|Main differences|In this)/i.test(trimmedLine) ||
        /^(The main|I've converted|As you can see)/i.test(trimmedLine)
      )) {
        break;
      }
      
      if (foundSQL) {
        sqlLines.push(line);
      }
    }

    return sqlLines.join('\n').trim();
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