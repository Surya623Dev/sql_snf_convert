import React from 'react';
import { Database, Code } from 'lucide-react';

const Header: React.FC = () => {
  return (
    <header className="bg-white shadow-lg border-b border-gray-200">
      <div className="container mx-auto px-4 py-6">
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-3">
            <div className="relative">
              <Database className="w-8 h-8 text-blue-600" />
              <Code className="w-4 h-4 text-teal-500 absolute -bottom-1 -right-1" />
            </div>
            <div>
              <h1 className="text-2xl font-bold text-gray-900">SQL Converter</h1>
              <p className="text-sm text-gray-600">Transform any SQL to Snowflake</p>
            </div>
          </div>
          
          <div className="hidden md:flex items-center space-x-4 text-sm text-gray-600">
            <div className="flex items-center space-x-2">
              <div className="w-2 h-2 bg-green-500 rounded-full"></div>
              <span>Multi-dialect support</span>
            </div>
            <div className="flex items-center space-x-2">
              <div className="w-2 h-2 bg-blue-500 rounded-full"></div>
              <span>Batch processing</span>
            </div>
            <div className="flex items-center space-x-2">
              <div className="w-2 h-2 bg-purple-500 rounded-full"></div>
              <span>Syntax highlighting</span>
            </div>
          </div>
        </div>
      </div>
    </header>
  );
};

export default Header;