import React, { useState } from 'react';
import { Download, Copy, FileText, AlertCircle, CheckCircle, RefreshCw, Archive } from 'lucide-react';
import { toast } from 'react-toastify';
import { ProcessedFile } from '../types';
import CodeEditor from './CodeEditor';

interface ConversionResultsProps {
  files: ProcessedFile[];
  onNewConversion: () => void;
}

const ConversionResults: React.FC<ConversionResultsProps> = ({ files, onNewConversion }) => {
  const [selectedFile, setSelectedFile] = useState<ProcessedFile | null>(files[0] || null);
  const [activeTab, setActiveTab] = useState<'original' | 'converted'>('converted');

  const successFiles = files.filter(f => f.status === 'success');
  const errorFiles = files.filter(f => f.status === 'error');

  const handleCopyToClipboard = (content: string) => {
    navigator.clipboard.writeText(content).then(() => {
      toast.success('Code copied to clipboard!');
    }).catch(() => {
      toast.error('Failed to copy to clipboard');
    });
  };

  const handleDownloadFile = (file: ProcessedFile) => {
    if (!file.convertedSql) return;
    
    const blob = new Blob([file.convertedSql], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = file.filename.replace(/\.sql$/i, '_snowflake.sql');
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
    
    toast.success(`Downloaded ${a.download}`);
  };

  const handleDownloadAll = async () => {
    if (successFiles.length === 0) {
      toast.error('No successfully converted files to download');
      return;
    }

    try {
      const response = await fetch('/api/download-zip', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ files: successFiles }),
      });

      if (!response.ok) {
        throw new Error('Failed to create ZIP file');
      }

      const blob = await response.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = 'converted_sql_files.zip';
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);

      toast.success(`Downloaded ZIP with ${successFiles.length} file(s)`);
    } catch (error) {
      console.error('Download error:', error);
      toast.error('Failed to download ZIP file');
    }
  };

  const formatFileSize = (bytes: number) => {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
  };

  return (
    <div className="max-w-7xl mx-auto">
      {/* Header */}
      <div className="bg-white rounded-lg shadow-sm p-6 mb-6">
        <div className="flex flex-col md:flex-row md:items-center md:justify-between">
          <div>
            <h2 className="text-2xl font-bold text-gray-900 mb-2">Conversion Results</h2>
            <div className="flex items-center space-x-4 text-sm">
              <div className="flex items-center space-x-1">
                <CheckCircle className="w-4 h-4 text-green-500" />
                <span className="text-green-700">{successFiles.length} successful</span>
              </div>
              {errorFiles.length > 0 && (
                <div className="flex items-center space-x-1">
                  <AlertCircle className="w-4 h-4 text-red-500" />
                  <span className="text-red-700">{errorFiles.length} failed</span>
                </div>
              )}
            </div>
          </div>
          
          <div className="flex items-center space-x-3 mt-4 md:mt-0">
            {successFiles.length > 1 && (
              <button
                onClick={handleDownloadAll}
                className="flex items-center space-x-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
              >
                <Archive className="w-4 h-4" />
                <span>Download All (ZIP)</span>
              </button>
            )}
            
            <button
              onClick={onNewConversion}
              className="flex items-center space-x-2 px-4 py-2 bg-gray-600 text-white rounded-lg hover:bg-gray-700 transition-colors"
            >
              <RefreshCw className="w-4 h-4" />
              <span>New Conversion</span>
            </button>
          </div>
        </div>
      </div>

      <div className="grid lg:grid-cols-4 gap-6">
        {/* File List */}
        <div className="lg:col-span-1">
          <div className="bg-white rounded-lg shadow-sm p-4">
            <h3 className="font-semibold text-gray-900 mb-4">Files ({files.length})</h3>
            
            <div className="space-y-2">
              {files.map((file, index) => (
                <button
                  key={index}
                  onClick={() => setSelectedFile(file)}
                  className={`
                    w-full text-left p-3 rounded-lg border transition-all duration-200
                    ${selectedFile === file 
                      ? 'border-blue-500 bg-blue-50 shadow-md' 
                      : 'border-gray-200 bg-white hover:border-gray-300'
                    }
                  `}
                >
                  <div className="flex items-start space-x-2">
                    <div className="flex-shrink-0 mt-1">
                      {file.status === 'success' ? (
                        <CheckCircle className="w-4 h-4 text-green-500" />
                      ) : (
                        <AlertCircle className="w-4 h-4 text-red-500" />
                      )}
                    </div>
                    <div className="flex-1 min-w-0">
                      <div className="text-sm font-medium text-gray-900 truncate">
                        {file.filename}
                      </div>
                      <div className="text-xs text-gray-500">
                        {formatFileSize(file.size)}
                      </div>
                      {file.error && (
                        <div className="text-xs text-red-600 mt-1">
                          {file.error}
                        </div>
                      )}
                    </div>
                  </div>
                </button>
              ))}
            </div>
          </div>
        </div>

        {/* Code Display */}
        <div className="lg:col-span-3">
          {selectedFile ? (
            <div className="bg-white rounded-lg shadow-sm overflow-hidden">
              {/* File Header */}
              <div className="border-b border-gray-200 p-4">
                <div className="flex items-center justify-between">
                  <div className="flex items-center space-x-3">
                    <FileText className="w-5 h-5 text-gray-600" />
                    <h3 className="font-semibold text-gray-900">{selectedFile.filename}</h3>
                    <div className={`px-2 py-1 rounded text-xs font-medium ${
                      selectedFile.status === 'success' 
                        ? 'bg-green-100 text-green-800' 
                        : 'bg-red-100 text-red-800'
                    }`}>
                      {selectedFile.status}
                    </div>
                  </div>
                  
                  {selectedFile.status === 'success' && (
                    <div className="flex items-center space-x-2">
                      <button
                        onClick={() => handleCopyToClipboard(selectedFile.convertedSql!)}
                        className="flex items-center space-x-1 px-3 py-1.5 text-sm bg-gray-100 text-gray-700 rounded hover:bg-gray-200 transition-colors"
                      >
                        <Copy className="w-3 h-3" />
                        <span>Copy</span>
                      </button>
                      
                      <button
                        onClick={() => handleDownloadFile(selectedFile)}
                        className="flex items-center space-x-1 px-3 py-1.5 text-sm bg-blue-100 text-blue-700 rounded hover:bg-blue-200 transition-colors"
                      >
                        <Download className="w-3 h-3" />
                        <span>Download</span>
                      </button>
                    </div>
                  )}
                </div>
                
                {/* Tabs */}
                {selectedFile.status === 'success' && (
                  <div className="flex space-x-1 mt-4">
                    <button
                      onClick={() => setActiveTab('converted')}
                      className={`px-3 py-1.5 text-sm rounded transition-colors ${
                        activeTab === 'converted'
                          ? 'bg-blue-100 text-blue-700 font-medium'
                          : 'text-gray-600 hover:text-gray-900'
                      }`}
                    >
                      Snowflake SQL
                    </button>
                    <button
                      onClick={() => setActiveTab('original')}
                      className={`px-3 py-1.5 text-sm rounded transition-colors ${
                        activeTab === 'original'
                          ? 'bg-blue-100 text-blue-700 font-medium'
                          : 'text-gray-600 hover:text-gray-900'
                      }`}
                    >
                      Original SQL
                    </button>
                  </div>
                )}
              </div>

              {/* Code Content */}
              <div className="h-96">
                {selectedFile.status === 'success' ? (
                  <CodeEditor
                    value={activeTab === 'converted' ? selectedFile.convertedSql! : selectedFile.originalSql}
                    language="sql"
                    readOnly={true}
                  />
                ) : (
                  <div className="p-6 h-full flex items-center justify-center">
                    <div className="text-center">
                      <AlertCircle className="w-12 h-12 text-red-500 mx-auto mb-4" />
                      <h4 className="font-semibold text-gray-900 mb-2">Conversion Failed</h4>
                      <p className="text-gray-600 mb-4">{selectedFile.error}</p>
                      <div className="bg-gray-50 rounded-lg p-4 max-w-md">
                        <h5 className="font-medium text-gray-900 mb-2">Original SQL:</h5>
                        <pre className="text-xs text-gray-700 overflow-auto max-h-32">
                          {selectedFile.originalSql}
                        </pre>
                      </div>
                    </div>
                  </div>
                )}
              </div>
            </div>
          ) : (
            <div className="bg-white rounded-lg shadow-sm p-12 text-center">
              <FileText className="w-12 h-12 text-gray-400 mx-auto mb-4" />
              <h3 className="text-lg font-medium text-gray-900 mb-2">Select a file to view</h3>
              <p className="text-gray-600">Choose a file from the list to see the conversion results</p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default ConversionResults;