import React from 'react';
import Editor from '@monaco-editor/react';

interface CodeEditorProps {
  value: string;
  language: string;
  readOnly?: boolean;
  onChange?: (value: string | undefined) => void;
}

const CodeEditor: React.FC<CodeEditorProps> = ({ 
  value, 
  language, 
  readOnly = false, 
  onChange 
}) => {
  return (
    <Editor
      height="100%"
      language={language}
      value={value}
      onChange={onChange}
      options={{
        readOnly,
        minimap: { enabled: false },
        fontSize: 14,
        lineNumbers: 'on',
        scrollBeyondLastLine: false,
        automaticLayout: true,
        tabSize: 2,
        wordWrap: 'on',
        theme: 'vs-light',
        padding: { top: 16, bottom: 16 },
        scrollbar: {
          vertical: 'auto',
          horizontal: 'auto',
        },
        renderWhitespace: 'selection',
        renderControlCharacters: false,
        contextmenu: !readOnly,
        selectOnLineNumbers: true,
        lineDecorationsWidth: 0,
        lineNumbersMinChars: 3,
      }}
      loading={
        <div className="flex items-center justify-center h-full">
          <div className="text-gray-500">Loading editor...</div>
        </div>
      }
    />
  );
};

export default CodeEditor;