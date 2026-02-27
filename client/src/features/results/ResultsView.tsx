import { useEffect, useMemo, useState } from 'react';
import { Icons } from '../../components/icons';
import { LatencyHistoryChart, QuantileChart, StatCard } from '../../components/metrics';
import { describeLogPayload, LOG_TYPE_CLASSES, LOG_TYPE_LABELS, renderLogMetadata } from '../../utils/ui';
import { fmt, summarizeJobLocal } from '../../utils/metrics';
import type {
  FileResult,
  JobHistoryEntry,
  JobSummary,
  RealtimeLatencySummary,
  RealtimeLogEntry,
  RealtimeLogSession,
  NormalizationConfig,
} from '../../types/app';

const describeNormalization = (norm?: NormalizationConfig): string => {
  if (!norm) return '未指定 (デフォルト)';
  const parts: string[] = [];
  if (norm.nfkc !== false) parts.push('NFKC');
  if (norm.stripPunct) parts.push('句読点除去');
  if (norm.stripSpace) parts.push('空白除去');
  if (norm.lowercase) parts.push('lowercase');
  if (parts.length === 0) return 'なし';
  return parts.join(' / ');
};

interface ResultsViewProps {
  jobHistory: JobHistoryEntry[];
  jobHistoryError: string | null;
  jobSummary: JobSummary | null;
  jobResults: FileResult[];
  lastJobId: string | null;
  loadJobData: (jobId: string) => Promise<void>;
  refreshJobHistory: () => Promise<void>;
  exportResults: (format: 'csv' | 'json') => void;
  latencyHistory: RealtimeLatencySummary[];
  refreshLatencyHistory: () => Promise<void>;
  logSessions: RealtimeLogSession[];
  logSessionsLoading: boolean;
  logSessionsError: string | null;
  refreshLogSessions: () => Promise<void>;
  fetchRealtimeLogs: (sessionId: string) => Promise<void>;
  logEntries: RealtimeLogEntry[];
  logLoading: boolean;
  logError: string | null;
  selectedLogSessionId: string | null;
}

export const ResultsView = ({
  jobHistory,
  jobHistoryError,
  jobSummary,
  jobResults,
  lastJobId,
  loadJobData,
  refreshJobHistory,
  exportResults,
  latencyHistory,
  refreshLatencyHistory,
  logSessions,
  logSessionsLoading,
  logSessionsError,
  refreshLogSessions,
  fetchRealtimeLogs,
  logEntries,
  logLoading,
  logError,
  selectedLogSessionId,
}: ResultsViewProps) => {
  const [resultsProviderFilter, setResultsProviderFilter] = useState('all');
  const [pathQuery, setPathQuery] = useState('');
  const [timezone, setTimezone] = useState<'local' | 'utc'>('local');

  useEffect(() => {
    if (jobHistory.length === 0) return;
    if (lastJobId && jobHistory.some((entry) => entry.jobId === lastJobId)) return;
    void loadJobData(jobHistory[0].jobId);
  }, [jobHistory, lastJobId, loadJobData]);

  const resultProviderOptions = useMemo(
    () => Array.from(new Set(jobResults.map((result) => result.provider))),
    [jobResults]
  );

  useEffect(() => {
    if (resultsProviderFilter !== 'all' && !resultProviderOptions.includes(resultsProviderFilter)) {
      setResultsProviderFilter('all');
    }
  }, [resultProviderOptions, resultsProviderFilter]);

  const filteredResults = useMemo(() => {
    const byProvider = resultsProviderFilter === 'all'
      ? jobResults
      : jobResults.filter((result) => result.provider === resultsProviderFilter);
    if (!pathQuery.trim()) return byProvider;
    const keyword = pathQuery.trim().toLowerCase();
    return byProvider.filter((result) => result.path.toLowerCase().includes(keyword));
  }, [jobResults, pathQuery, resultsProviderFilter]);

  const filteredSummary = useMemo(() => summarizeJobLocal(filteredResults), [filteredResults]);

  const providerSummaries = useMemo(
    () => resultProviderOptions.map((id) => ({
      provider: id,
      summary: summarizeJobLocal(jobResults.filter((result) => result.provider === id)),
    })),
    [jobResults, resultProviderOptions]
  );

  const latencyHistoryChrono = useMemo(() => [...latencyHistory].reverse(), [latencyHistory]);

  const latencyBySession = useMemo(() => new Map(latencyHistory.map((entry) => [entry.sessionId, entry])), [latencyHistory]);

  const normalizationLabel = useMemo(() => describeNormalization(jobResults[0]?.normalizationUsed), [jobResults]);

  const logSessionMeta = useMemo(() => {
    if (!selectedLogSessionId) return null;
    const latencyEntry = latencyHistory.find((entry) => entry.sessionId === selectedLogSessionId);
    if (latencyEntry) {
      return {
        provider: latencyEntry.provider,
        lang: latencyEntry.lang,
        startedAt: latencyEntry.startedAt,
      };
    }
    const fallback = logSessions.find((session) => session.sessionId === selectedLogSessionId);
    if (!fallback) return null;
    return {
      provider: fallback.provider,
      lang: fallback.lang,
      startedAt: fallback.startedAt ?? fallback.lastRecordedAt,
    };
  }, [latencyHistory, logSessions, selectedLogSessionId]);

  const formatDate = useMemo(() => (iso: string) => {
    const date = new Date(iso);
    if (timezone === 'utc') {
      return date.toLocaleString(undefined, { timeZone: 'UTC', hour12: false });
    }
    return date.toLocaleString();
  }, [timezone]);

  const downloadLatencyCsv = () => {
    if (latencyHistory.length === 0) return;
    const fields: Array<keyof RealtimeLatencySummary> = ['sessionId', 'provider', 'lang', 'count', 'avg', 'p50', 'p95', 'min', 'max', 'startedAt', 'endedAt'];
    const escapeCsv = (value: unknown) => {
      if (value == null) return '';
      if (typeof value === 'string') return `"${value.replace(/"/g, '""')}"`;
      return String(value);
    };
    const rows = latencyHistory.map((item) => fields.map((field) => escapeCsv(item[field])).join(','));
    const csv = [fields.join(','), ...rows].join('\n');
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'realtime-latency.csv';
    a.click();
    URL.revokeObjectURL(url);
  };

  const selectedHistoryEntry = jobHistory.find((entry) => entry.jobId === lastJobId);
  const summaryForDisplay = jobSummary ?? filteredSummary;

  return (
    <section>
      <div className="card controls-grid" style={{ alignItems: 'center', marginBottom: '2rem' }}>
        <div className="control-group">
          <label>ジョブ履歴</label>
          <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
            <select
              value={lastJobId ?? ''}
              onChange={(event) => {
                const value = event.target.value;
                if (value) {
                  void loadJobData(value);
                }
              }}
              style={{ flex: 1, minWidth: 200 }}
            >
              <option value="" disabled>
                {jobHistory.length === 0 ? '履歴がありません' : '履歴から選択'}
              </option>
              {jobHistory.map((entry) => (
                <option key={entry.jobId} value={entry.jobId}>
                  {`${(entry.providers ?? [entry.provider]).join(' vs ')} [${entry.status ?? 'completed'}] ${entry.done ?? 0}/${entry.total} (failed:${entry.failed ?? 0}) ${new Date(entry.updatedAt).toLocaleString()}`}
                </option>
              ))}
            </select>
            <button className="btn btn-ghost" onClick={() => refreshJobHistory()}>
              <Icons.Refresh /> 更新
            </button>
          </div>
          {jobHistoryError && (
            <div className="muted" style={{ fontSize: '0.75rem', marginTop: 4 }}>{jobHistoryError}</div>
          )}
        </div>
        <div className="control-group">
          <label>プロバイダーでフィルタ</label>
          <select value={resultsProviderFilter} onChange={(event) => setResultsProviderFilter(event.target.value)}>
            <option value="all">全てのプロバイダー</option>
            {resultProviderOptions.map((id) => (
              <option key={id} value={id}>{id}</option>
            ))}
          </select>
        </div>
        <div className="control-group">
          <label>ファイル名検索</label>
          <input type="search" placeholder="ファイル名を入力..." value={pathQuery} onChange={(event) => setPathQuery(event.target.value)} />
        </div>
      </div>

      {summaryForDisplay && (
        <div className="card" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: '1rem', flexWrap: 'wrap', marginBottom: '1.5rem' }}>
          <div className="muted">
            最新ジョブID: {lastJobId ?? 'なし'}
            {selectedHistoryEntry && (
              <span style={{ marginLeft: 8 }}>
                ({(selectedHistoryEntry.providers ?? [selectedHistoryEntry.provider]).join(' vs ')} · {selectedHistoryEntry.status ?? 'completed'} · {new Date(selectedHistoryEntry.updatedAt).toLocaleString()})
              </span>
            )}
            <div style={{ marginTop: 4, fontSize: '0.85rem' }}>正規化: {normalizationLabel}</div>
          </div>
          <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap' }}>
            <button className="btn btn-ghost" onClick={() => exportResults('csv')} disabled={!lastJobId}><Icons.Download /> CSV出力</button>
            <button className="btn btn-ghost" onClick={() => exportResults('json')} disabled={!lastJobId}><Icons.Download /> JSON出力</button>
          </div>
        </div>
      )}

      {(summaryForDisplay || filteredResults.length > 0) && (
        <>
          <h3 style={{ marginTop: '2rem' }}>パフォーマンス指標 (Performance Metrics)</h3>
          <div className="stat-grid">
            <StatCard title="平均 CER (文字誤り率)" value={fmt(summaryForDisplay?.cer.avg)} />
            <StatCard title="平均 WER (単語誤り率)" value={fmt(summaryForDisplay?.wer.avg)} />
            <StatCard title="平均 RTF (実時間係数)" value={fmt(summaryForDisplay?.rtf.avg)} />
            <StatCard title="平均 レイテンシ" value={fmt(summaryForDisplay?.latencyMs.avg)} unit="ms" />
            <StatCard title="Degraded 件数" value={String(summaryForDisplay?.degraded.count ?? 0)} />
            <StatCard
              title="Degraded 比率"
              value={
                summaryForDisplay ? `${(summaryForDisplay.degraded.ratio * 100).toFixed(1)}%` : '0.0%'
              }
            />
          </div>
        </>
      )}

      {providerSummaries.length > 0 && (
        <div className="chart-grid">
          <QuantileChart title="CER (文字誤り率) 分布" summaries={providerSummaries} selector={(s) => s.cer} />
          <QuantileChart title="WER (単語誤り率) 分布" summaries={providerSummaries} selector={(s) => s.wer} />
          <QuantileChart title="レイテンシ 分布" unit="ms" summaries={providerSummaries} selector={(s) => s.latencyMs} />
        </div>
      )}

      {filteredResults.length > 0 && (
        <div className="card" style={{ marginTop: '2rem', overflow: 'hidden', padding: 0 }}>
          <div style={{ padding: '1.5rem 1.5rem 0' }}>
            <h3>詳細データ</h3>
          </div>
          <div style={{ overflowX: 'auto' }}>
            <table>
              <thead>
                <tr>
                  <th>ファイル名</th>
                  <th>プロバイダー</th>
                  <th>CER</th>
                  <th>WER</th>
                  <th>RTF</th>
                  <th>レイテンシ</th>
                </tr>
              </thead>
              <tbody>
                {filteredResults.map((row) => (
                  <tr key={`${row.path}-${row.provider}`}>
                    <td style={{ fontFamily: 'var(--font-mono)', fontWeight: 500 }}>{row.path}</td>
                    <td><span className="msg-provider">{row.provider}</span></td>
                    <td>{fmt(row.cer)}</td>
                    <td>{fmt(row.wer)}</td>
                    <td>{fmt(row.rtf)}</td>
                    <td>{row.latencyMs ? `${row.latencyMs}ms` : '-'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {latencyHistory.length > 0 && (
        <div className="latency-chart-wrapper" style={{ marginTop: '2rem' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1.5rem', gap: '1rem', flexWrap: 'wrap' }}>
            <div>
              <h3>リアルタイムレイテンシ履歴</h3>
              <p className="muted" style={{ margin: 0 }}>直近 {latencyHistory.length} セッションの推移 (最新順)</p>
            </div>
            <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', alignItems: 'center' }}>
              <label className="muted" style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                タイムゾーン
                <select value={timezone} onChange={(event) => setTimezone(event.target.value as 'local' | 'utc')} style={{ padding: '4px 8px', borderRadius: 6 }}>
                  <option value="local">Local</option>
                  <option value="utc">UTC</option>
                </select>
              </label>
              <button className="btn btn-ghost" onClick={() => refreshLatencyHistory()}><Icons.Refresh /> 更新</button>
              <button className="btn btn-ghost" onClick={downloadLatencyCsv}><Icons.Download /> CSV</button>
            </div>
          </div>

          <LatencyHistoryChart history={latencyHistoryChrono} formatDate={formatDate} />

          <div style={{ overflowX: 'auto', marginTop: '1.5rem' }}>
            <table>
              <thead>
                <tr>
                  <th>セッションID</th>
                  <th>プロバイダー</th>
                  <th>言語</th>
                  <th>サンプル数</th>
                  <th>中央値 (p50)</th>
                  <th>95%値 (p95)</th>
                  <th>平均</th>
                  <th>開始時刻</th>
                  <th>ログ</th>
                </tr>
              </thead>
              <tbody>
                {latencyHistory.map((entry) => (
                  <tr key={entry.sessionId}>
                    <td style={{ fontFamily: 'var(--font-mono)' }}>{entry.sessionId.slice(0, 8)}</td>
                    <td><span className="msg-provider">{entry.provider}</span></td>
                    <td>{entry.lang}</td>
                    <td>{entry.count}</td>
                    <td>{fmt(entry.p50)}ms</td>
                    <td>{fmt(entry.p95)}ms</td>
                    <td>{fmt(entry.avg)}ms</td>
                    <td>{formatDate(entry.startedAt)}</td>
                    <td>
                      <button
                        type="button"
                        className="btn btn-ghost"
                        onClick={() => fetchRealtimeLogs(entry.sessionId)}
                        disabled={logLoading && selectedLogSessionId === entry.sessionId}
                      >
                        {selectedLogSessionId === entry.sessionId
                          ? logLoading
                            ? '取得中...'
                            : '再取得'
                          : '表示'}
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      <section className="card realtime-log-card" style={{ marginTop: '1.5rem' }}>
        <div className="realtime-log-card__header">
          <div>
            <h3 style={{ margin: '0.25rem 0' }}>ログセッション</h3>
            <p className="muted" style={{ margin: 0 }}>
              最新 {logSessions.length} 件のログセッション（latency 履歴はあるもののみ p50/p95 を表示）。
            </p>
          </div>
          <div className="realtime-log-card__actions">
            <button
              type="button"
              className="btn btn-ghost"
              onClick={() => refreshLogSessions()}
              disabled={logSessionsLoading}
            >
              <Icons.Refresh />
              更新
            </button>
          </div>
        </div>
        {logSessionsLoading && (
          <p className="muted" style={{ marginTop: '0.5rem' }}>
            ログセッションを読み込んでいます...
          </p>
        )}
        {logSessionsError && (
          <div className="banner error" style={{ marginTop: '1rem' }}>
            <div>{logSessionsError}</div>
          </div>
        )}
        {!logSessionsError && (
          <>
            {logSessions.length === 0 && !logSessionsLoading ? (
              <p className="muted" style={{ marginTop: '1rem', marginBottom: 0 }}>
                リアルタイムセッションを完了させるとログが蓄積されます。再度文字起こしを実行してください。
              </p>
            ) : (
              <div className="table-wrapper" style={{ marginTop: '1rem' }}>
                <table className="log-record-table">
                  <thead>
                    <tr>
                      <th>セッションID</th>
                      <th>プロバイダー</th>
                      <th>言語</th>
                      <th>エントリ数</th>
                      <th>最終更新</th>
                      <th>p50/p95/平均</th>
                      <th>ログ</th>
                    </tr>
                  </thead>
                  <tbody>
                    {logSessions.map((session) => {
                      const latencyEntry = latencyBySession.get(session.sessionId);
                      const latencySummary = latencyEntry
                        ? `${fmt(latencyEntry.p50)}/${fmt(latencyEntry.p95)}/${fmt(latencyEntry.avg)}`
                        : '-';
                      return (
                        <tr key={session.sessionId}>
                          <td style={{ fontFamily: 'var(--font-mono)' }}>{session.sessionId.slice(0, 8)}</td>
                          <td><span className="msg-provider">{session.provider}</span></td>
                          <td>{session.lang}</td>
                          <td>{session.entryCount}</td>
                          <td>{formatDate(session.lastRecordedAt)}</td>
                          <td>{latencySummary}</td>
                          <td>
                            <button
                              type="button"
                              className="btn btn-ghost"
                              onClick={() => fetchRealtimeLogs(session.sessionId)}
                              disabled={logLoading && selectedLogSessionId === session.sessionId}
                            >
                              {selectedLogSessionId === session.sessionId
                                ? logLoading
                                  ? '取得中...'
                                  : '再取得'
                                : '表示'}
                            </button>
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            )}
          </>
        )}
      </section>

      {selectedLogSessionId && (
        <section className="card realtime-log-card" style={{ marginTop: '1.5rem' }}>
          <div className="realtime-log-card__header">
            <div>
              <p className="muted" style={{ margin: 0 }}>
                セッションID: {selectedLogSessionId}
              </p>
              <h3 style={{ margin: '0.25rem 0' }}>セッションログ</h3>
              {logSessionMeta && (
                <p className="muted" style={{ margin: 0 }}>
                  {logSessionMeta.provider} / {logSessionMeta.lang} / 開始 {formatDate(logSessionMeta.startedAt)}
                </p>
              )}
            </div>
            <div className="realtime-log-card__actions">
              <button
                type="button"
                className="btn btn-ghost"
                onClick={() => selectedLogSessionId && fetchRealtimeLogs(selectedLogSessionId)}
                disabled={logLoading}
              >
                <Icons.Refresh />
                再取得
              </button>
            </div>
          </div>
          {logLoading && (
            <p className="muted" style={{ marginTop: '0.5rem' }}>
              ログを読み込んでいます...
            </p>
          )}
          {logError && (
            <div className="banner error" style={{ marginTop: '1rem' }}>
              <div>{logError}</div>
            </div>
          )}
          {!logError && (
            <>
              {logEntries.length === 0 && !logLoading ? (
                <p className="muted" style={{ marginTop: '1rem', marginBottom: 0 }}>
                  「ログ表示」ボタンを押してセッション単位の文字起こし/エラー履歴を取得できます。
                </p>
              ) : (
                logEntries.length > 0 && (
                  <div className="table-wrapper" style={{ marginTop: '1rem' }}>
                    <table className="log-record-table">
                      <thead>
                        <tr>
                          <th>取得時刻</th>
                          <th>種別</th>
                          <th>内容</th>
                          <th>レイテンシ</th>
                        </tr>
                      </thead>
                      <tbody>
                        {logEntries.map((entry, index) => {
                          const speakerId =
                            (entry.payload.type === 'transcript' || entry.payload.type === 'normalized') &&
                            (entry.payload as any).speakerId
                              ? String((entry.payload as any).speakerId)
                              : null;
                          return (
                            <tr key={`${entry.recordedAt}-${entry.payload.type}-${index}`}>
                              <td>{formatDate(entry.recordedAt)}</td>
                              <td>
                                <span className={`log-type ${LOG_TYPE_CLASSES[entry.payload.type]}`}>
                                  {LOG_TYPE_LABELS[entry.payload.type]}
                                </span>
                              </td>
                              <td>
                                <div style={{ display: 'flex', gap: '0.5rem', alignItems: 'center' }}>
                                  {speakerId ? <span className="speaker-badge">{speakerId}</span> : null}
                                  <span>{describeLogPayload(entry.payload)}</span>
                                </div>
                                {renderLogMetadata(entry.payload, formatDate)}
                              </td>
                              <td className="log-latency">
                                {(entry.payload.type === 'transcript' || entry.payload.type === 'normalized') &&
                                typeof (entry.payload as any).latencyMs === 'number'
                                  ? `${entry.payload.latencyMs}ms`
                                  : '-'}
                              </td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                )
              )}
            </>
          )}
        </section>
      )}
    </section>
  );
};
