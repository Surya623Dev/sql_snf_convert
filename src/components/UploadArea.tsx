import React, { useCallback, useState } from 'react';
import { useDropzone } from 'react-dropzone';
import { Upload, FileText, AlertCircle, Settings, Database } from 'lucide-react';
import { toast } from 'react-toastify';
import { ProcessedFile, SourceDialect } from '../types';
import DialectSelector from './DialectSelector';

interface UploadAreaProps {
  onFilesProcessed: (files: ProcessedFile[]) => void;
  onUploadStart: () => void;
  selectedDialect: string;
  onDialectChange: (dialect: string) => void;
}

const UploadArea: React.FC<UploadAreaProps> = ({ 
  onFilesProcessed, 
  onUploadStart,
  selectedDialect,
  onDialectChange
}) => {
  const [isUploading, setIsUploading] = useState(false);

  const onDrop = useCallback(async (acceptedFiles: File[]) => {
    if (acceptedFiles.length === 0) return;

    const sqlFiles = acceptedFiles.filter(file => 
      file.name.toLowerCase().endsWith('.sql')
    );

    if (sqlFiles.length === 0) {
      toast.error('Please upload only .sql files');
      return;
    }

    if (sqlFiles.length !== acceptedFiles.length) {
      toast.warning(`${acceptedFiles.length - sqlFiles.length} non-SQL files were ignored`);
    }

    setIsUploading(true);
    onUploadStart();
    
    toast.info(`Starting conversion of ${sqlFiles.length} file(s)...`);

    try {
      const formData = new FormData();
      sqlFiles.forEach(file => {
        formData.append('files', file);
      });
      formData.append('sourceDialect', selectedDialect);

      const response = await fetch('/api/convert', {
        method: 'POST',
        body: formData,
      });

      if (!response.ok) {
        throw new Error(`Server error: ${response.status}`);
      }

      const result = await response.json();
      
      const successCount = result.files.filter((f: ProcessedFile) => f.status === 'success').length;
      const errorCount = result.files.filter((f: ProcessedFile) => f.status === 'error').length;
      
      if (successCount > 0) {
        toast.success(`Successfully converted ${successCount} file(s)`);
      }
      if (errorCount > 0) {
        toast.error(`Failed to convert ${errorCount} file(s)`);
      }
      
      onFilesProcessed(result.files);
    } catch (error) {
      console.error('Upload error:', error);
      toast.error('Failed to process files. Please try again.');
    } finally {
      setIsUploading(false);
    }
  }, [selectedDialect, onFilesProcessed, onUploadStart]);

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    accept: {
      'text/plain': ['.sql'],
      'application/sql': ['.sql']
    },
    multiple: true,
    disabled: isUploading
  });

  return (
    <div className="max-w-4xl mx-auto">
      {/* Introduction */}
      <div className="text-center mb-8">
        <h2 className="text-3xl font-bold text-gray-900 mb-4">
          Convert SQL to Snowflake
        </h2>
        <p className="text-lg text-gray-600 max-w-2xl mx-auto">
          Upload your SQL files from MySQL, PostgreSQL, SQL Server, or Oracle and get 
          Snowflake-compatible code instantly. Support for batch processing and syntax highlighting.
        </p>
      </div>

      {/* Dialect Selector */}
      <div className="mb-6">
        <DialectSelector 
          selectedDialect={selectedDialect}
          onDialectChange={onDialectChange}
        />
      </div>

      {/* Upload Area */}
      <div
        {...getRootProps()}
        className={`
          relative border-2 border-dashed rounded-xl p-12 text-center cursor-pointer
          transition-all duration-200 ease-in-out
          ${isDragActive 
            ? 'border-blue-400 bg-blue-50 scale-105' 
            : 'border-gray-300 bg-white hover:border-blue-400 hover:bg-blue-50'
          }
          ${isUploading ? 'pointer-events-none opacity-75' : ''}
        `}
      >
        <input {...getInputProps()} />
        
        <div className="flex flex-col items-center space-y-4">
          {isUploading ? (
            <div className="animate-spin">
              <Settings className="w-12 h-12 text-blue-500" />
            </div>
          ) : (
            <Upload className={`w-12 h-12 ${isDragActive ? 'text-blue-500' : 'text-gray-400'}`} />
          )}
          
          <div>
            <h3 className="text-xl font-semibold text-gray-900 mb-2">
              {isUploading ? 'Processing files...' : 'Drop SQL files here'}
            </h3>
            {!isUploading && (
              <p className="text-gray-600">
                or <span className="text-blue-600 font-medium">click to browse</span>
              </p>
            )}
          </div>
          
          {!isUploading && (
            <div className="flex items-center space-x-4 text-sm text-gray-500">
              <div className="flex items-center space-x-1">
                <FileText className="w-4 h-4" />
                <span>SQL files only</span>
              </div>
              <div className="flex items-center space-x-1">
                <Upload className="w-4 h-4" />
                <span>Max 10MB per file</span>
              </div>
            </div>
          )}
        </div>

        {isDragActive && (
          <div className="absolute inset-0 bg-blue-400 bg-opacity-20 rounded-xl flex items-center justify-center">
            <div className="text-blue-600 font-medium">Drop files to convert</div>
          </div>
        )}
      </div>

      {/* Features */}
      <div className="mt-12 grid md:grid-cols-3 gap-6">
        <div className="text-center p-6 bg-white rounded-lg shadow-sm">
          <div className="w-12 h-12 bg-blue-100 rounded-lg flex items-center justify-center mx-auto mb-4">
            <Database className="w-6 h-6 text-blue-600" />
          </div>
          <h3 className="font-semibold text-gray-900 mb-2">Multi-Dialect Support</h3>
          <p className="text-sm text-gray-600">
            Convert from MySQL, PostgreSQL, SQL Server, and Oracle to Snowflake
          </p>
        </div>
        
        <div className="text-center p-6 bg-white rounded-lg shadow-sm">
          <div className="w-12 h-12 bg-green-100 rounded-lg flex items-center justify-center mx-auto mb-4">
            <FileText className="w-6 h-6 text-green-600" />
          </div>
          <h3 className="font-semibold text-gray-900 mb-2">Batch Processing</h3>
          <p className="text-sm text-gray-600">
            Upload multiple files at once and download as ZIP archive
          </p>
        </div>
        
        <div className="text-center p-6 bg-white rounded-lg shadow-sm">
          <div className="w-12 h-12 bg-purple-100 rounded-lg flex items-center justify-center mx-auto mb-4">
            <AlertCircle className="w-6 h-6 text-purple-600" />
          </div>
          <h3 className="font-semibold text-gray-900 mb-2">Error Handling</h3>
          <p className="text-sm text-gray-600">
            Detailed feedback and suggestions for problematic queries
          </p>
        </div>
      </div>
    </div>
  );
};

export default UploadArea;