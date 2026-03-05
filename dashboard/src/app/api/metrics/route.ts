import { NextResponse } from 'next/server';
import { pool } from '@/lib/db';

export async function GET() {
    try {
        const result = await pool.query(`
      SELECT 
        id, 
        run_id, 
        timestamp, 
        status, 
        sabotage_active, 
        time_to_restore_seconds, 
        total_rows_verified, 
        backup_size_mb 
      FROM lazarus_telemetry 
      ORDER BY timestamp DESC
      LIMIT 100
    `);

        // Calculate aggregated metrics
        const runs = result.rows;
        if (runs.length === 0) {
            return NextResponse.json({ runs: [], metrics: null });
        }

        const latest = runs[0];
        const sabotageRuns = runs.filter(r => r.sabotage_active);
        const sabotageCaught = sabotageRuns.filter(r => r.status === 'Failed_Verify').length;
        const sabotageCatchRate = sabotageRuns.length > 0
            ? (sabotageCaught / sabotageRuns.length) * 100
            : 100; // Default to 100 if none run

        const ttrTrends = [...runs].reverse().map(r => ({
            name: new Date(r.timestamp).toLocaleDateString(),
            ttr: parseFloat(r.time_to_restore_seconds),
            size: parseFloat(r.backup_size_mb)
        }));

        return NextResponse.json({
            runs,
            metrics: {
                latestStatus: latest.status,
                latestRows: latest.total_rows_verified,
                latestSize: parseFloat(latest.backup_size_mb),
                latestTtr: parseFloat(latest.time_to_restore_seconds),
                sabotageCatchRate: Math.round(sabotageCatchRate),
                ttrTrends
            }
        });

    } catch (error: any) {
        if (error.code === '42P01') {
            // Table does not exist (Airflow hasn't run yet)
            return NextResponse.json({ runs: [], metrics: null });
        }
        console.error("Failed to fetch telemetry", error);
        return NextResponse.json({ error: 'Internal Server Error' }, { status: 500 });
    }
}
