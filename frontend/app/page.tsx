'use client';

import React, { useState, useEffect } from 'react';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Card } from '@/components/ui/card';
import { Map, Cpu, MessageSquare, Compass, Github, FileText } from 'lucide-react';
import CommandCenter from '@/components/CommandCenter';
import GraphView from '@/components/GraphView';
import NavigatorChat from '@/components/NavigatorChat';
import Documentation from '@/components/Documentation';
import { NodeData, EdgeData } from '@/components/GraphView';
import { QueryResponse } from '@/lib/api';

export interface ChatMessage {
  role: 'user' | 'assistant';
  content: string;
  citations?: QueryResponse['citations'];
}

interface GraphData {
  nodes: Record<string, NodeData>;
  edges: EdgeData[];
}

export default function CartographerDashboard() {
  const [repoId, setRepoId] = useState<string | null>(null);
  const [graphData, setGraphData] = useState<GraphData | null>(null);
  const [activeTab, setActiveTab] = useState('orchestration');
  const [selectedNode, setSelectedNode] = useState<NodeData | null>(null);
  const [chatMessages, setChatMessages] = useState<ChatMessage[]>([]);

  // Load graph data when analysis completes or repoId changes
  useEffect(() => {
    if (repoId) {
      fetch(`http://localhost:8000/graph/${repoId}`)
        .then(res => res.json())
        .then(data => setGraphData(data))
        .catch(err => console.error('Failed to fetch graph:', err));
    }
  }, [repoId]);

  return (
    <main className="h-screen bg-slate-950 text-slate-200 p-4 font-sans selection:bg-blue-500/30 overflow-hidden flex flex-col">
      <div className="max-w-none w-full flex-1 flex flex-col min-h-0 space-y-4">
        
        {/* Header */}
        <header className="flex items-center justify-between pb-2 border-b border-slate-900/50">
          <div className="flex items-center gap-3">
            <div className="p-2 bg-blue-600 rounded-lg shadow-lg shadow-blue-600/20">
              <Compass className="w-6 h-6 text-white" />
            </div>
            <div>
              <h1 className="text-2xl font-black tracking-tighter text-white">CARTOGRAPHER</h1>
              <p className="text-slate-500 text-[10px] font-medium uppercase tracking-widest">Autonomous Codebase Intelligence</p>
            </div>
          </div>
          
          <div className="flex items-center gap-4">
            {repoId && (
              <div className="flex items-center gap-2 px-3 py-1 bg-slate-900 border border-slate-800 rounded-full text-xs">
                <Github className="w-3 h-3 text-slate-400" />
                <span className="text-slate-300 font-mono">{repoId}</span>
              </div>
            )}
            <div className="w-2 h-2 rounded-full bg-emerald-500 animate-pulse shadow-[0_0_8px_rgba(16,185,129,0.5)]" />
            <span className="text-[10px] uppercase font-bold tracking-widest text-slate-500">Live Engine</span>
          </div>
        </header>

        {/* Dynamic Tabs */}
        <Tabs defaultValue="orchestration" value={activeTab} onValueChange={setActiveTab} className="flex-1 flex flex-col min-h-0 space-y-4">
          <div className="flex-none flex justify-center">
            <TabsList className="bg-slate-900 border border-slate-800 p-1 h-10 shadow-xl">
              <TabsTrigger 
                value="orchestration" 
                className="data-[state=active]:bg-slate-800 data-[state=active]:text-white text-slate-400 hover:text-white px-6 text-xs transition-colors"
              >
                <Cpu className="w-3 h-3 mr-2" />
                Orchestration
              </TabsTrigger>
              <TabsTrigger 
                value="visualization" 
                disabled={!repoId} 
                className="data-[state=active]:bg-slate-800 data-[state=active]:text-white text-slate-400 hover:text-white px-6 text-xs transition-colors"
              >
                <Map className="w-3 h-3 mr-2" />
                Codebase Map
              </TabsTrigger>
              <TabsTrigger 
                value="docs" 
                disabled={!repoId} 
                className="data-[state=active]:bg-slate-800 data-[state=active]:text-white text-slate-400 hover:text-white px-6 text-xs transition-colors"
              >
                <FileText className="w-3 h-3 mr-2" />
                Docs
              </TabsTrigger>
              <TabsTrigger 
                value="navigator" 
                disabled={!repoId} 
                className="data-[state=active]:bg-slate-800 data-[state=active]:text-white text-slate-400 hover:text-white px-6 text-xs transition-colors"
              >
                <MessageSquare className="w-3 h-3 mr-2" />
                Navigator
              </TabsTrigger>
            </TabsList>
          </div>

          <div className="flex-1 min-h-0">
            <TabsContent value="orchestration" className="h-full m-0">
              <CommandCenter onAnalysisComplete={(id) => {
                setRepoId(id);
                setChatMessages([]); // Clear chat history for new repo
                setActiveTab('visualization');
              }} />
            </TabsContent>

            <TabsContent value="visualization" className="h-full m-0 relative">
              <Card className="w-full h-full bg-slate-950 border-slate-800 overflow-hidden shadow-2xl">
                <GraphView graphData={graphData} onNodeSelect={setSelectedNode} />
              </Card>
              
              {selectedNode && (
                <Card className="absolute top-4 right-4 w-72 max-h-[calc(100%-2rem)] bg-slate-900/90 backdrop-blur-xl border-slate-800 p-5 overflow-y-auto animate-in slide-in-from-right duration-300 shadow-2xl z-50 rounded-xl">
                  <div className="flex justify-between items-start mb-4">
                    <h3 className="font-bold text-blue-400 text-xs break-all leading-relaxed uppercase tracking-wider">{selectedNode.id}</h3>
                    <button onClick={() => setSelectedNode(null)} className="text-slate-500 hover:text-white transition-colors bg-slate-800 p-1 rounded-md ml-2">✕</button>
                  </div>
                  <div className="space-y-4">
                    <div className="bg-slate-950/50 p-3 rounded-lg border border-slate-800/50">
                      <span className="text-[9px] uppercase font-bold text-slate-500 tracking-[0.2em] block mb-1">Type</span>
                      <p className="text-[11px] text-white font-medium capitalize">{selectedNode.type}</p>
                    </div>
                    {selectedNode.path && (
                      <div className="bg-slate-950/50 p-3 rounded-lg border border-slate-800/50">
                        <span className="text-[9px] uppercase font-bold text-slate-500 tracking-[0.2em] block mb-1">File Path</span>
                        <p className="text-[11px] text-slate-300 truncate font-mono" title={selectedNode.path}>{selectedNode.path}</p>
                      </div>
                    )}
                    {selectedNode.purpose_statement && (
                      <div className="bg-slate-950/50 p-3 rounded-lg border border-slate-800/50">
                        <span className="text-[9px] uppercase font-bold text-slate-500 tracking-[0.2em] block mb-1">Purpose</span>
                        <p className="text-[11px] text-slate-400 leading-relaxed italic">{selectedNode.purpose_statement}</p>
                      </div>
                    )}
                    {selectedNode.complexity_score !== undefined && (
                      <div className="bg-slate-950/50 p-3 rounded-lg border border-slate-800/50">
                        <span className="text-[9px] uppercase font-bold text-slate-500 tracking-[0.2em] block mb-1">Complexity</span>
                        <div className="flex items-center gap-2">
                          <div className="flex-1 h-1 bg-slate-800 rounded-full overflow-hidden">
                            <div 
                              className="h-full bg-emerald-500" 
                              style={{ width: `${Math.min(100, selectedNode.complexity_score * 10)}%` }}
                             />
                          </div>
                          <span className="text-[11px] text-emerald-400 font-mono font-bold">{selectedNode.complexity_score.toFixed(1)}</span>
                        </div>
                      </div>
                    )}
                  </div>
                </Card>
              )}
            </TabsContent>

            <TabsContent value="docs" className="h-full m-0">
              <Documentation repoId={repoId} />
            </TabsContent>

            <TabsContent value="navigator" className="h-full m-0">
              <NavigatorChat 
                repoId={repoId} 
                messages={chatMessages}
                setMessages={setChatMessages}
              />
            </TabsContent>
          </div>
        </Tabs>
      </div>
    </main>
  );
}
