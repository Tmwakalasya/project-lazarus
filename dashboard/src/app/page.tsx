"use client";

import { useEffect, useState } from "react";
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, ReferenceLine
} from "recharts";
import {
  ShieldCheck, ShieldAlert, Activity, Database, Clock, TerminalSquare
} from "lucide-react";

export default function Home() {
  const [data, setData] = useState<{ runs: any[], metrics: any } | null>(null);

  useEffect(() => {
    // Poll the telemetry API
    const fetchMetrics = async () => {
      try {
        const res = await fetch("/api/metrics");
        const json = await res.json();
        setData(json);
      } catch (err) {
        console.error("Failed to load metrics", err);
      }
    };
    fetchMetrics();
    const int = setInterval(fetchMetrics, 10000);
    return () => clearInterval(int);
  }, []);

  if (!data || !data.metrics) {
    return (
      <div className="flex h-screen w-full items-center justify-center bg-dark-bg text-gray-400">
        <Activity className="animate-spin w-8 h-8 mr-3 text-brand-cyan" />
        Loading Telemetry...
      </div>
    );
  }

  const { metrics, runs } = data;
  const isHealthy = metrics.latestStatus === "Success";

  return (
    <main className="p-8 max-w-7xl mx-auto space-y-6">
      {/* Header */}
      <header className="flex justify-between items-center bg-glass-card border border-glass-border p-6 rounded-2xl backdrop-blur-md">
        <div>
          <h1 className="text-3xl font-bold bg-gradient-to-r from-brand-cyan to-brand-blue bg-clip-text text-transparent flex items-center">
            <TerminalSquare className="text-brand-cyan mr-3 w-8 h-8" />
            Lazarus Chaos Dashboard
          </h1>
          <p className="text-gray-400 mt-1">SRE Disaster Recovery Integrity Telemetry</p>
        </div>
        <div className={`px-4 py-2 rounded-full font-bold flex items-center shadow-lg ${isHealthy ? 'bg-emerald-500/10 text-emerald-400 border border-emerald-500/20' : 'bg-red-500/10 text-red-500 border border-red-500/20'}`}>
          {isHealthy ? <ShieldCheck className="w-5 h-5 mr-2" /> : <ShieldAlert className="w-5 h-5 mr-2" />}
          {isHealthy ? "SYSTEM SECURE" : "INTEGRITY BREACH"}
        </div>
      </header>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">

        {/* Metric 1 */}
        <div className="bg-glass-card border border-glass-border p-6 rounded-2xl">
          <div className="flex justify-between items-center mb-4">
            <h3 className="text-gray-400 font-medium">Rows Verified</h3>
            <Database className="w-5 h-5 text-purple-400" />
          </div>
          <div className="text-4xl font-bold text-white mb-2">
            {metrics.latestRows.toLocaleString()}
          </div>
          <p className="text-sm text-gray-400">Total records cross-checked</p>
        </div>

        {/* Metric 2 */}
        <div className="bg-glass-card border border-glass-border p-6 rounded-2xl relative overflow-hidden">
          <div className="flex justify-between items-center mb-4 relative z-10">
            <h3 className="text-gray-400 font-medium">Sabotage Catch Rate</h3>
            <ShieldCheck className="w-5 h-5 text-brand-cyan" />
          </div>
          <div className="text-4xl font-bold text-white mb-2 relative z-10">
            {metrics.sabotageCatchRate}%
          </div>
          <p className="text-sm text-gray-400 relative z-10">Chaos events successfully detected</p>

          {/* Decorative Gauge Glow */}
          <div className="absolute -bottom-10 -right-10 w-32 h-32 bg-brand-cyan rounded-full mix-blend-screen filter blur-[50px] opacity-30"></div>
        </div>

        {/* Metric 3 */}
        <div className="bg-glass-card border border-glass-border p-6 rounded-2xl">
          <div className="flex justify-between items-center mb-4">
            <h3 className="text-gray-400 font-medium">Backup Storage</h3>
            <Database className="w-5 h-5 text-emerald-400" />
          </div>
          <div className="text-4xl font-bold text-white mb-2">
            {metrics.latestSize > 0 ? metrics.latestSize.toFixed(2) : "0.00"} MB
          </div>
          <p className="text-sm text-gray-400">Total artifact footprint</p>
        </div>

      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* SLA Chart */}
        <div className="lg:col-span-2 bg-glass-card border border-glass-border p-6 rounded-2xl backdrop-blur-md">
          <div className="flex justify-between items-center mb-6">
            <div>
              <h2 className="text-xl font-bold text-white">Time to Restore (TTR)</h2>
              <p className="text-sm text-gray-400 mt-1">SLA Target: {"<"} 60 seconds</p>
            </div>
            <Clock className="text-yellow-400 w-5 h-5" />
          </div>
          <div className="h-[300px] w-full">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={metrics.ttrTrends}>
                <XAxis dataKey="name" stroke="#52525b" tick={{ fill: '#a1a1aa' }} />
                <YAxis stroke="#52525b" tick={{ fill: '#a1a1aa' }} />
                <Tooltip
                  contentStyle={{ backgroundColor: '#18181b', border: '1px solid rgba(255,255,255,0.1)', borderRadius: '8px' }}
                  itemStyle={{ color: '#06b6d4' }}
                />
                <ReferenceLine y={60} label="SLA 60s" stroke="#eab308" strokeDasharray="3 3" />
                <Line
                  type="monotone"
                  dataKey="ttr"
                  stroke="#06b6d4"
                  strokeWidth={3}
                  dot={{ fill: '#3b82f6', strokeWidth: 2, r: 4 }}
                  activeDot={{ r: 8 }}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Pipeline Health Heatmap */}
        <div className="bg-glass-card border border-glass-border p-6 rounded-2xl backdrop-blur-md flex flex-col">
          <h2 className="text-xl font-bold text-white mb-6">Recent Executions</h2>
          <div className="flex-grow flex flex-col gap-3 overflow-y-auto pr-2">
            {runs.slice(0, 8).map((run: any) => (
              <div key={run.id} className="flex justify-between items-center p-3 rounded-xl bg-white/5 border border-white/5">
                <div>
                  <div className="text-white text-sm font-medium">Run #{run.run_id.split('__')[0] || run.run_id.substring(0, 8)}</div>
                  <div className="text-xs text-gray-400">{new Date(run.timestamp).toLocaleString()}</div>
                </div>
                <div className={`text-xs px-2 py-1 rounded-full font-semibold ${run.status === 'Success' ? 'bg-emerald-500/20 text-emerald-400' :
                    run.status === 'Failed_Verify' ? 'bg-amber-500/20 text-amber-400' :
                      'bg-red-500/20 text-red-400'
                  }`}>
                  {run.status === 'Failed_Verify' ? 'Sabotage Caught' : run.status}
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </main>
  );
}
