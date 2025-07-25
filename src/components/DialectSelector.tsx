import React from 'react';
import { Database } from 'lucide-react';

interface DialectSelectorProps {
  selectedDialect: string;
  onDialectChange: (dialect: string) => void;
}

const dialects = [
  { value: 'mysql', label: 'MySQL', color: 'bg-orange-100 text-orange-800' },
  { value: 'postgresql', label: 'PostgreSQL', color: 'bg-blue-100 text-blue-800' },
  { value: 'sqlserver', label: 'SQL Server', color: 'bg-red-100 text-red-800' },
  { value: 'oracle', label: 'Oracle', color: 'bg-purple-100 text-purple-800' },
];

const DialectSelector: React.FC<DialectSelectorProps> = ({ 
  selectedDialect, 
  onDialectChange 
}) => {
  return (
    <div className="bg-white rounded-lg shadow-sm p-6">
      <div className="flex items-center space-x-2 mb-4">
        <Database className="w-5 h-5 text-gray-600" />
        <h3 className="text-lg font-semibold text-gray-900">Source Database</h3>
      </div>
      
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        {dialects.map((dialect) => (
          <button
            key={dialect.value}
            onClick={() => onDialectChange(dialect.value)}
            className={`
              px-4 py-3 rounded-lg border-2 transition-all duration-200
              ${selectedDialect === dialect.value
                ? 'border-blue-500 bg-blue-50 shadow-md scale-105'
                : 'border-gray-200 bg-white hover:border-gray-300 hover:shadow-sm'
              }
            `}
          >
            <div className={`inline-block px-2 py-1 rounded text-sm font-medium mb-1 ${dialect.color}`}>
              {dialect.label}
            </div>
            <div className="text-xs text-gray-500">
              {selectedDialect === dialect.value ? 'Selected' : 'Click to select'}
            </div>
          </button>
        ))}
      </div>
    </div>
  );
};

export default DialectSelector;