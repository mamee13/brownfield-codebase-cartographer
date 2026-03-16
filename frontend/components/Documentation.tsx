'use client';

import React, { useState, useEffect, useCallback } from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { Card } from '@/components/ui/card';
import { ScrollArea } from '@/components/ui/scroll-area';
import { FileText, BookOpen, Loader2 } from 'lucide-react';
import { api } from '@/lib/api';

interface DocumentationProps {
  repoId: string | null;
}

export default function Documentation({ repoId }: DocumentationProps) {
  const [activeDoc, setActiveDoc] = useState<'codebase.md' | 'onboarding_brief.md'>('onboarding_brief.md');
  const [content, setContent] = useState<string>('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const loadDoc = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await api.getDoc(repoId!, activeDoc);
      setContent(data);
    } catch (_err) {
      setError(`Failed to load ${activeDoc}`);
    } finally {
      setLoading(false);
    }
  }, [repoId, activeDoc]);

  useEffect(() => {
    if (repoId) {
      loadDoc();
    }
  }, [repoId, activeDoc, loadDoc]);

  if (!repoId) {
    return (
      <div className="h-full flex items-center justify-center text-slate-500 italic">
        Select or analyze a repository to view documentation.
      </div>
    );
  }

  return (
    <div className="h-full flex flex-col gap-4">
      <div className="flex items-center gap-2">
        <button
          onClick={() => setActiveDoc('onboarding_brief.md')}
          className={`flex items-center gap-2 px-4 py-2 rounded-lg text-xs font-bold transition-all ${
            activeDoc === 'onboarding_brief.md'
              ? 'bg-blue-600 text-white shadow-lg shadow-blue-600/20'
              : 'bg-slate-900 text-slate-400 hover:text-white border border-slate-800'
          }`}
        >
          <BookOpen className="w-4 h-4" />
          Onboarding Brief
        </button>
        <button
          onClick={() => setActiveDoc('codebase.md')}
          className={`flex items-center gap-2 px-4 py-2 rounded-lg text-xs font-bold transition-all ${
            activeDoc === 'codebase.md'
              ? 'bg-blue-600 text-white shadow-lg shadow-blue-600/20'
              : 'bg-slate-900 text-slate-400 hover:text-white border border-slate-800'
          }`}
        >
          <FileText className="w-4 h-4" />
          Codebase Analysis
        </button>
      </div>

      <Card className="flex-1 bg-slate-900/50 border-slate-800 overflow-hidden flex flex-col relative min-h-0 shadow-inner">
        {loading && (
          <div className="absolute inset-0 bg-slate-950/80 backdrop-blur-md z-10 flex items-center justify-center">
            <div className="flex flex-col items-center gap-4">
              <Loader2 className="w-10 h-10 text-blue-500 animate-spin" />
              <p className="text-blue-400 font-mono text-xs animate-pulse">READING MANIFEST...</p>
            </div>
          </div>
        )}

        {error ? (
          <div className="flex-1 flex items-center justify-center text-red-400 p-8 text-center font-mono text-xs">
            <span className="bg-red-500/10 border border-red-500/20 px-4 py-2 rounded-lg">
              {error}
            </span>
          </div>
        ) : (
          <ScrollArea className="flex-1 h-full bg-slate-950/20">
            <div className="max-w-4xl mx-auto px-12 py-10 doc-container">
              <ReactMarkdown remarkPlugins={[remarkGfm]}>
                {content}
              </ReactMarkdown>
            </div>
          </ScrollArea>
        )}
      </Card>

      <style jsx global>{`
        .doc-container {
          color: #f1f5f9 !important; /* Explicit slate-100 */
          line-height: 1.75;
          font-size: 1rem;
        }
        .doc-container h1 { 
          color: #ffffff !important; 
          font-weight: 900; 
          font-size: 2.25rem; 
          margin-bottom: 2rem; 
          border-bottom: 2px solid #1e293b; 
          padding-bottom: 0.75rem; 
          letter-spacing: -0.05em;
        }
        .doc-container h2 { 
          color: #60a5fa !important; /* blue-400 */
          font-weight: 800; 
          font-size: 1.5rem; 
          margin-top: 3rem; 
          margin-bottom: 1.25rem; 
          text-transform: uppercase; 
          letter-spacing: 0.05em;
          border-left: 4px solid #2563eb;
          padding-left: 1rem;
        }
        .doc-container h3 { 
          color: #ffffff !important; 
          font-weight: 700; 
          font-size: 1.25rem; 
          margin-top: 2rem; 
          margin-bottom: 1rem;
        }
        .doc-container p { 
          color: #e2e8f0 !important; /* slate-200 */
          margin-bottom: 1.5rem;
        }
        .doc-container ul, .doc-container ol { 
          margin-bottom: 1.5rem; 
          padding-left: 1.5rem;
          color: #e2e8f0 !important;
        }
        .doc-container li {
          margin-bottom: 0.5rem;
        }
        .doc-container li::marker {
          color: #3b82f6;
          font-weight: bold;
        }
        .doc-container code { 
          background-color: #1e293b !important; 
          color: #93c5fd !important; 
          padding: 0.2rem 0.4rem; 
          border-radius: 0.375rem; 
          font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
          font-size: 0.875em;
          border: 1px solid #334155;
        }
        .doc-container pre { 
          background-color: #020617 !important; 
          border: 1px solid #1e293b; 
          border-radius: 0.75rem; 
          padding: 1.5rem; 
          margin-bottom: 1.5rem; 
          overflow-x: auto;
          box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.5);
        }
        .doc-container pre code { 
          background-color: transparent !important; 
          color: #34d399 !important; /* emerald-400 */
          padding: 0; 
          border: none;
        }
        .doc-container blockquote { 
          border-left: 4px solid #334155; 
          padding: 1rem 1.5rem; 
          margin: 1.5rem 0; 
          background-color: rgba(15, 23, 42, 0.5); 
          color: #94a3b8 !important; 
          font-style: italic;
          border-top-right-radius: 0.5rem;
          border-bottom-right-radius: 0.5rem;
        }
        .doc-container table { 
          width: 100%; 
          border-collapse: collapse; 
          margin-bottom: 2rem; 
          border: 1px solid #1e293b;
          border-radius: 0.5rem;
          overflow: hidden;
        }
        .doc-container th { 
          background-color: #0f172a; 
          color: #f8fafc !important; 
          padding: 0.75rem; 
          text-align: left; 
          border-bottom: 2px solid #1e293b;
          font-size: 0.75rem;
          text-transform: uppercase;
          letter-spacing: 0.1em;
        }
        .doc-container td { 
          padding: 0.75rem; 
          border-bottom: 1px solid #1e293b; 
          color: #cbd5e1 !important;
          font-size: 0.875rem;
        }
        .doc-container strong { 
          color: #ffffff !important; 
          font-weight: 700;
        }
        .doc-container a { 
          color: #60a5fa !important; 
          text-decoration: underline;
        }
        .doc-container a:hover {
          color: #93c5fd !important;
        }
      `}</style>
    </div>
  );
}
