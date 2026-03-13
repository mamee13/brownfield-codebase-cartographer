'use client';

import React, { useMemo, useRef, useState } from 'react';
import {
  ReactFlow,
  Background,
  Controls,
  Panel,
} from '@xyflow/react';
import { Maximize, Minimize } from 'lucide-react';
import '@xyflow/react/dist/style.css';

export interface NodeData {
  id: string;
  path?: string;
  type?: string;
  purpose_statement?: string;
  complexity_score?: number;
}

export interface EdgeData {
  source: string;
  target: string;
  type?: string;
}

interface GraphViewProps {
  graphData: {
    nodes: Record<string, NodeData>;
    edges: EdgeData[];
  } | null;
  onNodeSelect?: (node: NodeData) => void;
}

const getDeterministicPosition = (id: string, index: number) => {
  let hash = 0;
  for (let i = 0; i < id.length; i++) {
    hash = id.charCodeAt(i) + ((hash << 5) - hash);
  }
  
  const cols = 6;
  const col = index % cols;
  const row = Math.floor(index / cols);
  
  return {
    x: col * 280 + (Math.abs(hash % 100)),
    y: row * 180 + (Math.abs((hash >> 8) % 100)),
  };
};

const getNodeColor = (type?: string) => {
  switch (type) {
    case 'module': return '#1e40af'; // Blue
    case 'dataset': return '#065f46'; // Emerald
    case 'transformation': return '#5b21b6'; // Violet
    default: return '#334155';
  }
};

export default function GraphView({ graphData, onNodeSelect }: GraphViewProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [isFullscreen, setIsFullscreen] = useState(false);

  const toggleFullscreen = () => {
    if (!containerRef.current) return;

    if (!document.fullscreenElement) {
      containerRef.current.requestFullscreen().catch((err) => {
        console.error(`Error attempting to enable fullscreen: ${err.message}`);
      });
      setIsFullscreen(true);
    } else {
      document.exitFullscreen();
      setIsFullscreen(false);
    }
  };

  const nodes = useMemo(() => {
    if (!graphData || !graphData.nodes) return [];
    return Object.values(graphData.nodes).map((node, idx) => ({
      id: node.id,
      data: { label: node.path || node.id.split(':').pop() },
      position: getDeterministicPosition(node.id, idx),
      style: {
        background: getNodeColor(node.type),
        color: '#fff',
        border: '1px solid #475569',
        borderRadius: '8px',
        padding: '12px',
        fontSize: '11px',
        fontWeight: '500',
        width: 200,
        textAlign: 'center' as const,
        boxShadow: '0 10px 15px -3px rgb(0 0 0 / 0.1)',
        cursor: 'pointer',
      },
    }));
  }, [graphData]);

  const edges = useMemo(() => {
    if (!graphData || !graphData.edges) return [];
    return graphData.edges.map((edge, idx) => ({
      id: `edge-${idx}`,
      source: edge.source,
      target: edge.target,
      animated: edge.type === 'imports' || edge.type === 'produces',
      style: { stroke: '#64748b', strokeWidth: 2 },
      label: edge.type,
      labelStyle: { fill: '#94a3b8', fontSize: '10px', fontWeight: 'bold' },
    }));
  }, [graphData]);

  return (
    <div ref={containerRef} className="w-full h-full bg-slate-950 overflow-hidden relative">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        colorMode="dark"
        fitView
        panOnScroll={true}
        selectionOnDrag={false}
        onNodeClick={(_, node) => {
          if (onNodeSelect && graphData?.nodes[node.id]) {
            onNodeSelect(graphData.nodes[node.id]);
          }
        }}
      >
        <Background gap={25} color="#1e293b" />
        <Controls />
        <Panel position="top-right" className="flex items-center gap-2 bg-slate-900/90 backdrop-blur-md border border-slate-800 p-2 rounded-lg shadow-2xl">
          <span className="text-[10px] uppercase tracking-wider font-bold text-slate-400 px-2 border-r border-slate-800">
            Interactive Map
          </span>
          <button 
            onClick={toggleFullscreen}
            className="p-1 hover:bg-slate-800 rounded transition-colors text-slate-400 hover:text-white"
            title={isFullscreen ? "Exit Fullscreen" : "Fullscreen"}
          >
            {isFullscreen ? <Minimize className="w-4 h-4" /> : <Maximize className="w-4 h-4" />}
          </button>
        </Panel>
      </ReactFlow>
    </div>
  );
}
