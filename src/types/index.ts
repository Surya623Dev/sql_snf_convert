export interface ProcessedFile {
  filename: string;
  originalSql: string;
  convertedSql: string | null;
  status: 'success' | 'error' | 'processing';
  error?: string;
  size: number;
}

export interface ConversionResponse {
  files: ProcessedFile[];
}

export type SourceDialect = 'mysql' | 'postgresql' | 'sqlserver' | 'oracle';