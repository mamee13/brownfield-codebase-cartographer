'use client';

import React, { useState, useRef, useEffect } from 'react';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Input } from '@/components/ui/input';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Card } from '@/components/ui/card';
import { Send, Bot, User, FileCode, Loader2 } from 'lucide-react';
import { api } from '@/lib/api';
import { ChatMessage } from '@/app/page';

interface NavigatorChatProps {
  repoId: string | null;
  messages: ChatMessage[];
  setMessages: React.Dispatch<React.SetStateAction<ChatMessage[]>>;
}

export default function NavigatorChat({ repoId, messages, setMessages }: NavigatorChatProps) {
  const [input, setInput] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const scrollRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (scrollRef.current) {
      const scrollContainer = scrollRef.current.querySelector('[data-radix-scroll-area-viewport]');
      if (scrollContainer) {
        scrollContainer.scrollTop = scrollContainer.scrollHeight;
      }
    }
  }, [messages, isLoading]);

  const handleSend = async () => {
    if (!input.trim() || !repoId) return;

    const userMsg = input;
    setInput('');
    setMessages((prev) => [...prev, { role: 'user', content: userMsg }]);
    setIsLoading(true);

    try {
      const cartographyDir = `.cartography_repos/${repoId}`;
      const response = await api.query(cartographyDir, userMsg);
      setMessages((prev) => [
        ...prev,
        { role: 'assistant', content: response.answer, citations: response.citations },
      ]);
    } catch (err: unknown) {
      const errorMessage = err instanceof Error ? err.message : 'Sorry, I encountered an error while processing your request.';
      setMessages((prev) => [
        ...prev,
        { role: 'assistant', content: `Error: ${errorMessage}` },
      ]);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <Card className="flex flex-col h-full bg-slate-950 border-slate-800 overflow-hidden relative">
      <div className="flex-none p-4 border-b border-slate-800 flex items-center justify-between bg-slate-950/50 backdrop-blur-md z-10">
        <div className="flex items-center gap-2">
          <Bot className="w-5 h-5 text-blue-400" />
          <h2 className="font-semibold text-slate-100">Navigator Chat</h2>
        </div>
        {repoId && (
          <Badge variant="outline" className="text-[10px] font-mono bg-blue-500/10 text-blue-400 border-blue-500/20">
            {repoId}
          </Badge>
        )}
      </div>

      <ScrollArea className="flex-1 min-h-0" ref={scrollRef}>
        <div className="p-4 space-y-4 pb-24">
          {messages.length === 0 && (
            <div className="text-center py-20 text-slate-600 text-sm italic">
              <Bot className="w-12 h-12 mx-auto mb-4 opacity-10" />
              Ask me anything about the codebase...
            </div>
          )}
          {messages.map((msg, idx) => (
            <div
              key={idx}
              className={`flex gap-3 animate-in fade-in slide-in-from-bottom-2 duration-300 ${
                msg.role === 'user' ? 'justify-end' : 'justify-start'
              }`}
            >
              <div
                className={`max-w-[85%] rounded-2xl p-4 shadow-lg ${
                  msg.role === 'user'
                    ? 'bg-blue-600 text-white rounded-tr-none'
                    : 'bg-slate-900 text-slate-200 border border-slate-800 rounded-tl-none'
                }`}
              >
                <div className="flex items-center gap-2 mb-2">
                  {msg.role === 'user' ? <User className="w-4 h-4 opacity-70" /> : <Bot className="w-4 h-4 text-blue-400" />}
                  <span className="text-[10px] uppercase font-black tracking-widest opacity-50">
                    {msg.role}
                  </span>
                </div>
                <div className="text-sm leading-relaxed whitespace-pre-wrap">{msg.content}</div>
                
                {msg.citations && msg.citations.length > 0 && (
                  <div className="mt-4 pt-3 border-t border-slate-800/50 flex flex-wrap gap-2">
                    {msg.citations.map((cite, cIdx) => (
                      <Badge key={cIdx} variant="outline" className="text-[10px] bg-black/40 border-slate-700 text-slate-400 hover:text-white hover:border-slate-500 transition-colors flex items-center gap-1">
                        <FileCode className="w-3 h-3" />
                        {cite.file} ({cite.line_range})
                      </Badge>
                    ))}
                  </div>
                )}
              </div>
            </div>
          ))}
          {isLoading && (
            <div className="flex gap-3 justify-start">
              <div className="bg-slate-900 border border-slate-800 rounded-2xl rounded-tl-none p-4 w-1/2 animate-pulse">
                <div className="flex items-center gap-2 mb-3">
                  <Loader2 className="w-3 h-3 text-blue-500 animate-spin" />
                  <div className="h-2 w-16 bg-slate-800 rounded"></div>
                </div>
                <div className="space-y-2">
                  <div className="h-3 w-full bg-slate-800 rounded"></div>
                  <div className="h-3 w-4/5 bg-slate-800 rounded"></div>
                </div>
              </div>
            </div>
          )}
        </div>
      </ScrollArea>

      <div className="absolute bottom-0 left-0 right-0 p-4 bg-linear-to-t from-slate-950 via-slate-950/95 to-transparent pt-10">
        <div className="flex gap-2 bg-slate-900 border border-slate-800 p-2 rounded-xl shadow-2xl backdrop-blur-sm">
          <Input
            placeholder={repoId ? "Type your prompt..." : "Analyze a repo first"}
            value={input}
            onChange={(e) => setInput(e.target.value)}
            disabled={!repoId || isLoading}
            onKeyDown={(e) => e.key === 'Enter' && handleSend()}
            className="flex-1 bg-transparent border-none text-slate-100 focus-visible:ring-0 focus-visible:ring-offset-0 placeholder:text-slate-600"
          />
          <Button 
            onClick={handleSend} 
            disabled={!repoId || isLoading || !input.trim()} 
            size="icon"
            className={`transition-all duration-300 ${!input.trim() ? 'bg-slate-800 text-slate-600' : 'bg-blue-600 text-white hover:bg-blue-500 shadow-lg shadow-blue-600/20'}`}
          >
            {isLoading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Send className="w-4 h-4" />}
          </Button>
        </div>
      </div>
    </Card>
  );
}
