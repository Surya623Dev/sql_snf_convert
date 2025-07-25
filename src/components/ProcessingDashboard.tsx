import React from 'react';
import { Loader2, Database } from 'lucide-react';

const ProcessingDashboard: React.FC = () => {
  return (
    <div className="max-w-2xl mx-auto text-center">
      <div className="bg-white rounded-xl shadow-lg p-12">
        <div className="flex justify-center mb-6">
          <div className="relative">
            <Database className="w-16 h-16 text-blue-600" />
            <Loader2 className="w-6 h-6 text-blue-500 animate-spin absolute -top-1 -right-1" />
          </div>
        </div>
        
        <h2 className="text-2xl font-bold text-gray-900 mb-4">
          Converting Your SQL Files
        </h2>
        
        <p className="text-gray-600 mb-8">
          Please wait while we process your files and convert them to Snowflake-compatible SQL. 
          This usually takes just a few seconds.
        </p>
        
        <div className="space-y-4">
          <div className="flex items-center justify-center space-x-2 text-sm text-gray-500">
            <div className="w-2 h-2 bg-blue-500 rounded-full animate-pulse"></div>
            <span>Analyzing SQL syntax</span>
          </div>
          
          <div className="flex items-center justify-center space-x-2 text-sm text-gray-500">
            <div className="w-2 h-2 bg-green-500 rounded-full animate-pulse" style={{ animationDelay: '0.5s' }}></div>
            <span>Applying conversion rules</span>
          </div>
          
          <div className="flex items-center justify-center space-x-2 text-sm text-gray-500">
            <div className="w-2 h-2 bg-purple-500 rounded-full animate-pulse" style={{ animationDelay: '1s' }}></div>
            <span>Optimizing for Snowflake</span>
          </div>
        </div>
      </div>
    </div>
  );
};

export default ProcessingDashboard;