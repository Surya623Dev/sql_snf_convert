import React, { useState } from 'react';
import { ToastContainer } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';
import Header from './components/Header';
import UploadArea from './components/UploadArea';
import ProcessingDashboard from './components/ProcessingDashboard';
import ConversionResults from './components/ConversionResults';
import { ProcessedFile } from './types';

function App() {
  const [processedFiles, setProcessedFiles] = useState<ProcessedFile[]>([]);
  const [currentView, setCurrentView] = useState<'upload' | 'processing' | 'results'>('upload');
  const [selectedDialect, setSelectedDialect] = useState<string>('mysql');

  const handleFilesProcessed = (files: ProcessedFile[]) => {
    setProcessedFiles(files);
    setCurrentView('results');
  };

  const handleNewConversion = () => {
    setProcessedFiles([]);
    setCurrentView('upload');
  };

  const handleUploadStart = () => {
    setCurrentView('processing');
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 to-indigo-100">
      <Header />
      
      <main className="container mx-auto px-4 py-8">
        {currentView === 'upload' && (
          <UploadArea 
            onFilesProcessed={handleFilesProcessed}
            onUploadStart={handleUploadStart}
            selectedDialect={selectedDialect}
            onDialectChange={setSelectedDialect}
          />
        )}
        
        {currentView === 'processing' && (
          <ProcessingDashboard />
        )}
        
        {currentView === 'results' && (
          <ConversionResults 
            files={processedFiles}
            onNewConversion={handleNewConversion}
          />
        )}
      </main>

      <ToastContainer
        position="top-right"
        autoClose={4000}
        hideProgressBar={false}
        newestOnTop
        closeOnClick
        rtl={false}
        pauseOnFocusLoss
        draggable
        pauseOnHover
        theme="light"
        className="mt-16"
      />
    </div>
  );
}

export default App;