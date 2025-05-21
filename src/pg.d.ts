// src/pg.d.ts
import type { Pool, QueryResult, QueryResultRow } from 'pg';

declare module './pg.js' {
  export const pool: Pool;
  export function query<R extends QueryResultRow = any, I extends any[] = any[]>(text: string, params?: I): Promise<QueryResult<R>>;
  export function closePool(): Promise<void>;
}
