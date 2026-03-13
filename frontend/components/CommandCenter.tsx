'use client';

import React, { useState, useRef, useEffect } from 'react';
import { Card } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Button } from '@/components/ui/button';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Badge } from '@/components/ui/badge';
import { Play, Loader2, CheckCircle2, AlertCircle, Cpu } from 'lucide-react';
import { api, AgentProgress } from '@/lib/api';

interface CommandCenterProps {
  onAnalysisComplete: (repoId: string) => void;
}

export default function CommandCenter({ onAnalysisComplete }: CommandCenterProps) {
  const [repoUrl, setRepoUrl] = useState('');
  const [isAnalyzing, setIsAnalyzing] = useState(false);
  const [incremental, setIncremental] = useState(true);
  const [logs, setLogs] = useState<{ type: string; msg: string }[]>([]);
  const [recentRepos, setRecentRepos] = useState<string[]>([]);
  const scrollRef = useRef<HTMLDivElement>(null);

  // Load existing repos on mount
  useEffect(() => {
    api.listRepos().then(setRecentRepos).catch(console.error);
  }, []);

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTo(0, scrollRef.current.scrollHeight);
    }
  }, [logs]);

  const handleStartAnalysis = async () => {
    setLogs([{ type: 'info', msg: `Starting ${incremental ? 'incremental' : 'full'} analysis for ${repoUrl}...` }]);
    setIsAnalyzing(true);

    try {
      const { repo_id } = await api.analyze(repoUrl, incremental);
      
      api.streamAnalysis(repo_id, (progress: AgentProgress) => {
        if (progress.event === 'progress') {
          setLogs((prev) => [...prev, { type: 'agent', msg: progress.data }]);
        } else if (progress.event === 'complete') {
          setLogs((prev) => [...prev, { type: 'success', msg: progress.data }]);
          setIsAnalyzing(false);
          // Refresh list
          api.listRepos().then(setRecentRepos);
          onAnalysisComplete(repo_id);
        } else if (progress.event === 'error') {
          setLogs((prev) => [...prev, { type: 'error', msg: progress.data }]);
          setIsAnalyzing(false);
        }
      });
    } catch (err) {
      setLogs((prev) => [...prev, { type: 'error', msg: 'Failed to connect to backend' }]);
      setIsAnalyzing(false);
    }
  };

  return (
    <Card className="p-6 bg-slate-950 border-slate-800 h-full flex flex-col">
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-3">
          <Cpu className="w-6 h-6 text-emerald-400" />
          <h2 className="text-xl font-bold text-slate-100">Agent Command Center</h2>
        </div>
      </div>

      <div className="flex flex-col gap-4 mb-6">
        <div className="flex gap-2">
          <Input
            placeholder="Enter Git Repository URL..."
            value={repoUrl}
            onChange={(e) => setRepoUrl(e.target.value)}
            disabled={isAnalyzing}
            className="bg-slate-900 border-slate-800 text-slate-100"
          />
          <Button 
            onClick={handleStartAnalysis} 
            disabled={isAnalyzing || !repoUrl.trim()}
            className="bg-emerald-600 hover:bg-emerald-700 text-white min-w-[120px]"
          >
            {isAnalyzing ? <Loader2 className="w-4 h-4 animate-spin mr-2" /> : <Play className="w-4 h-4 mr-2" />}
            {isAnalyzing ? 'Analyzing...' : 'Start'}
          </Button>
        </div>

        <div className="flex items-center gap-4 bg-slate-900/50 p-2 rounded-lg border border-slate-800/50">
          <button
            onClick={() => setIncremental(!incremental)}
            disabled={isAnalyzing}
            className="flex items-center gap-2 group"
          >
            <div className={`w-10 h-5 rounded-full transition-colors relative ${incremental ? 'bg-emerald-600' : 'bg-slate-700'}`}>
              <div className={`absolute top-1 w-3 h-3 bg-white rounded-full transition-all ${incremental ? 'left-6' : 'left-1'}`} />
            </div>
            <span className="text-[10px] uppercase font-bold text-slate-400 group-hover:text-slate-200 transition-colors">
              Incremental Analysis
            </span>
          </button>
          <div className="border-l border-slate-800 h-4" />
          <p className="text-[9px] text-slate-500 italic">
            {incremental 
              ? "Reuses existing module metadata to speed up analysis." 
              : "Performs a complete fresh scan of all files."}
          </p>
        </div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6 flex-1 min-h-0">
        <div className="flex flex-col min-h-0">
          <div className="flex items-center justify-between mb-2 px-1">
            <span className="text-xs font-bold uppercase tracking-widest text-slate-500">Live Agent Activity</span>
            {isAnalyzing && (
              <Badge variant="outline" className="animate-pulse bg-emerald-500/10 text-emerald-400 border-emerald-500/20 text-[10px]">
                Processing
              </Badge>
            )}
          </div>
          
          <ScrollArea className="flex-1 bg-black/40 border border-slate-800 rounded-lg p-4 font-mono text-xs" ref={scrollRef}>
            <div className="space-y-2">
              {logs.length === 0 && (
                <div className="text-slate-700 italic">No activity yet. Enter a URL to begin.</div>
              )}
              {logs.map((log, idx) => (
                <div key={idx} className="flex gap-2">
                  <span className="text-slate-600">[{new Date().toLocaleTimeString([], { hour12: false })}]</span>
                  {log.type === 'agent' && <span className="text-blue-400">❖</span>}
                  {log.type === 'success' && <CheckCircle2 className="w-3 h-3 text-emerald-400 mt-0.5" />}
                  {log.type === 'error' && <AlertCircle className="w-3 h-3 text-red-500 mt-0.5" />}
                  <span className={`${
                    log.type === 'agent' ? 'text-slate-300' : 
                    log.type === 'success' ? 'text-emerald-300 font-bold' :
                    log.type === 'error' ? 'text-red-400' :
                    'text-slate-500'
                  }`}>
                    {log.msg}
                  </span>
                </div>
              ))}
            </div>
          </ScrollArea>
        </div>

        <div className="flex flex-col min-h-0">
          <div className="flex items-center justify-between mb-2 px-1">
            <span className="text-xs font-bold uppercase tracking-widest text-slate-500">Recent Repositories</span>
          </div>
          <ScrollArea className="flex-1 bg-slate-900/50 border border-slate-800 rounded-lg p-2 font-mono text-xs">
            <div className="space-y-1">
              {recentRepos.length === 0 && (
                <div className="text-center py-10 text-slate-700 italic">No analyzed repos found.</div>
              )}
              {recentRepos.map((repo) => (
                <button
                  key={repo}
                  onClick={() => onAnalysisComplete(repo)}
                  className="w-full text-left p-2 rounded hover:bg-slate-800 text-slate-400 hover:text-white transition-colors flex items-center justify-between group"
                >
                  <span className="truncate pr-2">{repo}</span>
                  <Play className="w-3 h-3 opacity-0 group-hover:opacity-100 text-emerald-500" />
                </button>
              ))}
            </div>
          </ScrollArea>
        </div>
      </div>
    </Card>
  );
}
