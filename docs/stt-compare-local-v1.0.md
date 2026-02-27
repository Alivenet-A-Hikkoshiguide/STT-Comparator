# STT-Compare Local 仕様 v1.0
作成日: 2025-12-26

## 0. 概要
### 0.1 背景と目的
複数の音声認識（ASR）プロバイダ／エンジンを同一条件で比較し、リアルタイム（マイク入力）とバッチ（録音ファイル）の双方で精度・レイテンシ・実用性を評価するローカル起動のミニアプリを提供する。本仕様では要件（機能・非機能）とアーキテクチャ（構成・データフロー・インタフェース）を統合定義する。

### 0.2 成果物（Deliverables）
- ローカル実行可能なアプリ（Web UI + ローカルサーバ）
- プロバイダ差し替え可能な Adapter 群（現行バンドルは Deepgram / ElevenLabs / OpenAI / local_whisper / whisper_streaming。`mock` は任意追加）
- 評価モジュール（WER/CER/レイテンシ/RTFなど）
- 設定ファイル・環境変数テンプレート・README
- サンプルデータと評価マニフェスト（JSON）雛形
- 自動テスト（ユニット／統合）一式

### 0.3 成功基準（Acceptance Goals）
- UI上からプロバイダ切替が可能で、マイク入力のリアルタイム転写が動作する
- 録音ファイルの一括転写と**自動採点（CER/WER）**が可能
- 同一PCM条件での公平比較（16kHz/16bit/mono など）を担保
- レポート（JSON/CSV）のエクスポートが可能

## 1. スコープ
### 1.1 対象
- リアルタイムASR（マイク入力 → UI へ部分／確定結果表示）
- バッチASR（WAV等のファイル → テキスト）
- 音声会話（Voice Agent: STT/LLM/TTS の簡易対話。ローカル用途向け）
- 評価（CER/WER/RTF/各種遅延・安定性）
- 結果の保存・可視化・エクスポート
- 複数ASRプロバイダのプラガブル化

### 1.2 非対象（当面）
- 高度な会話理解・要約・翻訳（Voice Agent は簡易対話のみ）
- 自動話者分離・音源分離（将来拡張）
- クラウド側のジョブ管理／コスト最適化
- 商用配布用インストーラ（必要なら Tauri/Electron 包材は拡張）

### 1.3 前提（ローカル限定・非本番）
- 本アプリはローカル利用を前提とし、高可用性・冗長化・RBAC/監査ログなど本番運用向けの高度化は対象外。
- 最低限維持する安全・公平性: 16k/mono/PCM統一、.env による鍵管理とコミット禁止、許可済みASRエンドポイントのみ送信、パス/入力サニタイズ、CORS/helmet既定を緩めない。

## 2. 利用シナリオ／ユースケース
- U1: 評価者がアプリを起動 → マイクで各プロバイダのリアルタイム転写を比較。
- U2: 研究者が録音データセット（GT付き）を読み込み、一括転写 → 自動採点 → 結果をCSV化。
- U3: 実務担当が辞書（コンテキスト）や句読点ポリシーの有無で条件分岐評価を実施。

## 3. 用語集（抜粋）
- CER: Character Error Rate（文字誤り率）
- WER: Word Error Rate（単語誤り率）
- RTF: Real-time Factor（処理時間／音声長）
- Interim / Final: 部分結果／確定結果
- Adapter: プロバイダ差異を吸収する実装

## 4. 前提・制約
- ローカル起動: Web UI とローカルサーバ（Node.js）は利用者マシン上で動作。外部通信は各ASRプロバイダのAPIエンドポイントまたはローカルASRサーバ（local_whisper, nvidia_riva等）のみ。
- 同一PCM条件での比較（16kHz, 16-bit, mono 推奨）。
- 評価時の正規化は NFKC + 句読点除去を基本とし、単語境界の公平性を保つため空白は保持する（stripSpace=false がデフォルト）。
- ブラウザ音声取得は AudioWorklet（PCM16LE）→ WS で PCM フレーム送信（推奨）。互換として webm/opus を送ってサーバ側で FFmpeg 変換する経路も残す。
- サポートブラウザ: Chrome / Edge 最新版。Safari 等は v1.0 では対象外。
- OS: Windows / macOS / Linux（Node.js v20+ / FFmpeg が必要）。
- 日本語評価は CER、英語系は WER を主指標（切替可能）。

## 5. 非機能要件
### 5.1 性能
- マイク → 部分結果: 平均 < 800ms、p95 < 1.5s（サーバ計測レイテンシ）。
- ファイル一括: 100件×30秒で平均 RTF ≤ 1.0 を目標。処理完了し再現可能。
- UI操作の応答: 主要操作 <100ms 体感。

### 5.2 信頼性・可用性
- 回線断時は明示エラー＋再接続ボタン。
- バッチ処理は jobId 単位で再実行可能（途中再開は対象外、同条件で再評価）。

### 5.3 セキュリティ・プライバシ
- API鍵は .env 等で管理し平文コミット禁止。
- 収集ログは匿名化（デフォルトで音声は保存しない／オプトインで保存）。
- 送信先ドメイン制限（許可リスト）。
- ローカルユースのため本番級の多層防御は省略するが、鍵漏えい防止とパストラバーサル防止は必須。

### 5.4 運用性
- ログレベル切替（ERROR/INFO/DEBUG）。
- 設定ホットリロード（可能な範囲）。
- 評価結果の保存はローカルだが肥大化防止のため保持期間/件数を設定可能（デフォルト: 30日または10万件）。

## 6. 全体アーキテクチャ
```
Web UI (React/TS) -> WS(JSON config + binary PCM frames) -> Local Node Server
Node Server: Transmux (FFmpeg), Router, Scoring, Storage (JSONL/SQLite), CSV/JSON export
Adapters: deepgram / elevenlabs / openai / local_whisper（Batchのみ） / whisper_streaming（Realtime/Batch） / mock（ローカル確認/テスト用のスタブ。デフォルト設定には含めていない）。他クラウドは任意拡張（デフォルト無効）
```

## 7. モジュール構成と責務
- Web UI: マイク制御、WS接続、部分／確定表示、条件選択、バッチ評価UI、結果可視化。
  - 句読点ポリシー（none/basic/full）をUIで切替可能。
- Voice Agent: STT/LLM/TTS の簡易対話、barge-in、会議音声入力／出力のルーティング。
- Transmux: 互換モード（webm/opus 等）の場合は FFmpeg → PCM(16k mono, linear16) 変換し Adapter に送る。preview/replay/batch のアップロードファイルも受信直後に FFmpeg の strict フラグ（`-v error -xerror -err_detect explode` など）で 16k mono PCM WAV に正規化し、デコードエラーや 100ms 未満の出力しか得られない壊れた音源は **preview/replay では 422**（`AUDIO_UNSUPPORTED_FORMAT` / `AUDIO_TOO_LONG`）で拒否する。batch は投入時に拡張子/MIME を検証し、非音声ファイルを含む場合は 400 でジョブ作成を拒否し、実行中のデコード失敗はファイル単位で失敗として記録する。必要に応じて strict decode 失敗時に degraded 変換へフォールバックし、`degraded:true` を返す。正規化パラメータは `config.json.ingressNormalize`（サンプルレート/チャンネル/ピークヘッドルーム）で統一管理する。
- Router: ProviderId 解決、セッションライフサイクル管理、コンテキスト管理。
- Provider Adapter: API/SDK/WS/gRPC への接続、PCM送信、結果正規化、supportsStreaming/batch を表現。
- Scoring: 正規化、CER/WER/RTF 算出、レイテンシ算出、p50/p95 集計。
- Storage: JSONL/SQLite 永続化、CSV/JSON エクスポート。

## 8. データフロー（シーケンス）
### 8.1 リアルタイム（マイク）
1. Start → AudioWorklet（PCM収録）開始 → WS 接続。
   - 単一: `/ws/stream?provider=<id>&lang=<bcp47>`
   - 比較: `/ws/stream/compare?providers=<id1>,<id2>&lang=<bcp47>`
   - チャネル分離（L/R）は単一プロバイダ時のみ対応（`/ws/stream/compare` では非対応）。
2. 接続直後に StreamingConfigMessage（`pcm:true` + `clientSampleRate` 必須）を送信。
3. 約250msごとに PCM16LE のフレームをバイナリ送信（ヘッダ + PCM）。
4. サーバで必要に応じて provider 要求サンプルレートへリサンプル → Adapter.startStreaming → PCM を逐次送信。
5. Adapter が Interim/Final を PartialTranscript に正規化。
6. Server → UI: StreamSessionMessage を送信後、StreamTranscriptMessage/NormalizedTranscriptMessage(JSON) を送信し、`latencyMs`/`originCaptureTs` を付与（captureTs ベース）。
7. UI 表示、エラーはトースト。
8. Stop で Worklet 停止 → Adapter end → WS close。

### 8.2 バッチ
1. UI で files[] と ref_json（任意）を指定し Run。並列度 options.parallel は「同時に処理するファイル数」を意味し、複数プロバイダー指定時はサーバ側で provider 数までは自動で引き上げ（config.jobs.maxParallel を上限）て同一ファイルを並列に転写し、公平な比較を担保する。`jobs.maxParallel` はジョブ単位ではなくサーバ全体の同時実行上限として適用する。
2. POST `/api/jobs/transcribe` (multipart/form-data): files[], provider, lang, ref_json, options。
3. サーバ: jobId 発行 → 各ファイルを FFmpeg で PCM 化 → Adapter.transcribeFileFromPCM → Scoring → Storage に保存。
4. UI: GET `/api/jobs/:jobId/status` で進捗ポーリング。
5. 完了後 GET `/api/jobs/:jobId/results?format=csv|json` で取得し表示・DL。

## 9. インタフェース仕様（厳密）
### 9.1 WebSocket
- パス:
  - `/ws/stream?provider=<id>&lang=<bcp47>`（単一ストリーム）
  - `/ws/stream/compare?providers=<id1>,<id2>&lang=<bcp47>`（比較ストリーム）
  - `/ws/replay?provider=<id>&lang=<bcp47>&sessionId=<id>`（内部再生・単一）
  - `/ws/replay?providers=<id1>,<id2>&lang=<bcp47>&sessionId=<id>`（内部再生・比較）
  - `/ws/voice?lang=<bcp47>`（音声会話）
- 接続直後: UI → config JSON → 音声バイナリ送信 → Server → StreamServerMessage（voice は VoiceServerMessage）。
- StreamingConfigMessage 例:
```
{
  "type": "config",
  "enableInterim": true,
  "contextPhrases": ["サンプル"],
  "normalizePreset": "ja_cer",
  "pcm": true,
  "clientSampleRate": 16000,
  "channels": 1,
  "options": { "enableVad": false, "meetingMode": true, "punctuationPolicy": "full", "parallel": 1 }
}
```
- 音声（推奨 / UI既定）: PCM16LE フレーム（~250ms）。
  - フレーム構造（Little Endian）: `seq(uint32)` + `captureTs(float64, ms)` + `durationMs(float32)` + `pcm(bytes)`
  - `captureTs` は「当該チャンク末尾の壁時計(ms)」を推奨（レイテンシ算出の基準）。
  - `pcm:true` のとき `clientSampleRate` は必須（サーバは provider ごとに必要ならリサンプル）。
- 音声（互換 / ツール用途）: `pcm` を省略して webm/opus など圧縮チャンクを送ることも可能（サーバが FFmpeg で PCM 化する）。この場合 `captureTs` が無いので `latencyMs` はサーバ受信時刻ベースになる。
- `options.meetingMode:true` のとき、サーバはバックログが一定以上に積み上がると一部のPCMチャンクをドロップして追従する（会議のリアルタイム性を優先）。ドロップが過剰な場合は `error` で切断し、`degraded:true` を付与してUIへ通知する。
- Server → UI transcript 例:
```
{
  "type": "transcript",
  "provider": "deepgram",
  "isFinal": false,
  "text": "こんにちは",
  "words": [{ "startSec": 0.10, "endSec": 0.35, "text": "こんにち", "confidence": 0.86 }],
  "timestamp": 1732000000000,
  "channel": "mic",
  "originCaptureTs": 1732000000000,
  "latencyMs": 420
}
```
- エラー: `{ "type": "error", "message": "..." }`

### 9.2 HTTP（バッチ / Realtime / Voice）
- GET `/api/config` → `{ audio: { chunkMs } }`
- GET `/api/providers` → Provider availability + capability flags
- POST `/api/jobs/transcribe` (multipart/form-data): files[], (provider | providers), lang, ref_json (任意), options (任意)
  - `provider`: 単一プロバイダ
  - `providers`: 複数プロバイダ（同一ファイルを同条件で並列評価し、結果は provider ごとに1行ずつ返る）
  - 非音声ファイルが混在する場合は 400（`invalidFiles[]`）を返し、ジョブは作成しない。
  - `ref_json` 指定時はジョブ投入前に manifest coverage を検証し、マッピング不能がある場合は 400（`missingFiles[]`, `ambiguousFiles[]`）を返す。
  - Response: `{ "jobId": "uuid", "queued": 10 }`
- GET `/api/jobs` → ジョブ履歴一覧（`status/done/failed/errors/warnings` を含み、結果0件の完全失敗ジョブも返す）
- GET `/api/jobs/:jobId/status` → `{ jobId, total, done, failed, status, providers, errors, warnings }`
  - `warnings[]`: `{ code, message, createdAt, details? }`
  - `code='parallel_clamped'`: 要求並列度が `jobs.maxParallel` / CPU 上限で抑制されたことを通知。
  - `code='degraded_audio_detected'`: degraded 正規化が混在したことを通知（比較時に要確認）。
- GET `/api/jobs/:jobId/results?format=csv|json`
  - CSV カラム: path,provider,lang,cer,wer,rtf,latency_ms,audio_sample_rate_hz,audio_channels,audio_encoding,degraded,normalization,text,ref_text
  - JSON 例: `{ path, provider, lang, durationSec, processingTimeMs, rtf, cer, wer, latencyMs, text, refText, normalizationUsed, vendorProcessingMs, audioSpec, createdAt, opts }`
  - CSVはsnake_case、JSONはcamelCase。
- GET `/api/jobs/:jobId/summary` → `{ count, cer:{n,avg,p50,p95}, wer:{...}, rtf:{...}, latencyMs:{...}, degraded:{count,ratio} }`
  - `degraded.count`: degraded 正規化が使われた結果件数
  - `degraded.ratio`: `degraded.count / count`
  - `groupBy=provider` を指定すると provider 別集計を返す。
- GET `/api/realtime/latency?limit=20` → 直近セッションのレイテンシ集計
- GET `/api/realtime/log-sessions?limit=50` → セッション一覧
- GET `/api/realtime/logs/:sessionId` → セッションログ
- POST `/api/realtime/preview` (multipart/form-data: file) → `{ previewId, previewUrl, degraded }`
- GET `/api/realtime/preview/:id` → audio/wav
- POST `/api/realtime/replay` (multipart/form-data: file, provider|providers, lang) → `{ sessionId, filename, createdAt, degraded }`
- GET `/api/voice/status` → Voice preset availability / missing env

### 9.5 リアルタイム: 内部再生
- `POST /api/realtime/replay` (multipart/form-data: `file`, `provider` または `providers`, `lang`) でファイルをアップロードし `sessionId` を返却。プロバイダは `streaming` 判定済みである必要があり、サーバは `ReplaySessionStore` に一時保存（TTL 5 分）し、録音なしでも同じ FFmpeg → Adapter の経路を再現する。
- UI は返却された `sessionId` を使って `WS /ws/replay?...` に接続し、通常の `StreamingConfigMessage` を送信。
  - 単一: `/ws/replay?provider=<id>&lang=<bcp47>&sessionId=<id>`
  - 比較: `/ws/replay?providers=<id1>,<id2>&lang=<bcp47>&sessionId=<id>`
  - WS はファイルから PCM を `controller.sendAudio` で送出し、`StreamTranscriptMessage` には `channel:'file'` を付与する。接続終了後、セッションファイルは削除される。
- CLI の `scripts/realtime-replay.ts` も同様の `StreamingConfigMessage`/WS を使うため、UI の内部再生をベースにスクリプトを組み合わせることでマイクが不要な検証も行える。

### 9.6 リアルタイム: ログ
- `GET /api/realtime/log-sessions?limit=50` — セッション一覧（最新順）を返す。
- `GET /api/realtime/logs/:sessionId` — 指定セッションの `session`, `transcript`, `normalized`, `error`, `session_end` を時系列で返す。生成AIなどへ渡すため、1セッション分の `StreamTranscriptMessage`（partial/final）、`latencyMs`、`channel`、`provider`、`timestamp` を含むタイムラインをサーバ側で蓄積しておける。
- ログ行は `storage.driver` に応じて `runs/<date>/realtime-logs.jsonl`（JSONL）または SQLite 内の `realtime_logs` テーブルへ書きこまれ、`storage.retentionDays`/`storage.maxRows` の範囲で pruning される。JSONL/SQLite ともに `recordedAt` を基準に retention を判定する。`storage.path` を共有するバッチ/latency ログと同じストレージなので、生成AIレビューのために `runs` の任意の日付フォルダを丸ごとコピーしてもよい。
- エントリ構造: `{ sessionId, provider, lang, recordedAt, payload }`。`payload` は `StreamSessionMessage` (`startedAt`)、`StreamTranscriptMessage`（`text`/`latencyMs`/`isFinal`/`speakerId?`）、`NormalizedTranscriptMessage`（`textDelta?`/`speakerId?` など）、`StreamErrorMessage`（`message`）、`StreamSessionEndMessage`（`endedAt`）のいずれか。
### 9.3 評価マニフェスト
```
{
  "version": 1,
  "language": "ja-JP",
  "items": [ { "audio": "dataset/a.wav", "ref": "これはテストです。" } ],
  "normalization": { "nfkc": true, "stripPunct": true, "stripSpace": true }
}
```
`sample-data/manifest.example.json` はテンプレート用の例であり、音声ファイルは同梱していないため `items[].audio` に対応するファイルを用意して下さい。

### 9.4 Realtime Replay Helper
- `scripts/realtime-replay.ts` は UI の `/ws/stream`（PCMフレーム）送信経路を TypeScript で模倣し、`ffmpeg -re` で `sample-data` などのファイルを PCM16LE に変換して 250ms フレームとして直接ストリーミングします。このスクリプトを使えば、外部マイクを使えない静音環境でもプロバイダ・言語・マニフェストごとのレイテンシを再現性高く収集できます。
- 起動: `pnpm replay:realtime --provider mock --language ja-JP --file sample-data/your.wav`（`--manifest sample-data/manifest.example.json` も可）。`--enable-interim`/`--normalize-preset`/`--punctuation`/`--context`/`--dictionary` や `--dry-run` により、StreamingConfigMessage のパラメタを UI と同じ感覚で調整できます。
- 出力: `StreamTranscriptMessage` を受信するたびに `latencyMs` を含むログ行（`interim` と `final`）、最後に平均/p95/最大の latency 要約、transcript count、final count を表示します。`GET /api/realtime/latency?limit=20` と併せて session-level の集計と突合させれば、実験比較の信頼性が高まります。
- プロバイダごと: Deepgram では `.env` に `DEEPGRAM_API_KEY`、ElevenLabs では `ELEVENLABS_API_KEY`、`whisper_streaming` では faster-whisper-server が起動済みで `WHISPER_STREAMING_READY_URL` が通る状態であることを確認してからスクリプトを叩いてください。`mock` でまず挙動を確認し、必要なら manifest の `items[]` にノイズ・会話・朗読などのサンプルを追加して比較軸を整えてください。
- `items[].audio` はアップロード `files[]` のファイル名と一致させる。`dataset/a.wav` のようにディレクトリ付きで記述しても basename が一意なら照合できるが、basename が複数候補に衝突する場合は曖昧として 400 を返す。

## 10. 型仕様（TypeScript）
  - v1.0 デフォルト: `deepgram`, `elevenlabs`, `openai`, `local_whisper`, `whisper_streaming`（ローカル常駐 WS/HTTP サーバ, Streaming/Batch 両対応）。`mock` はローカル検証やテスト用途で必要に応じて追加してください。その他は拡張用。
  - `whisper_streaming`: faster-whisper-server 等のローカル常駐エンドポイントを前提。WS 初期メッセージで language/task/model を送信し、partial/final を受信する。
- StreamingOptions: language, sampleRateHz(16000), encoding 'linear16', enableInterim?, contextPhrases?
- TranscriptWord, PartialTranscript (timestamp, channel 'mic'|'file')
- PunctuationPolicy: 'none' | 'basic' | 'full'; TranscriptionOptions: enableVad?, punctuationPolicy?, dictionaryPhrases?, parallel?
- StreamingConfigMessage { type:'config', enableInterim?, contextPhrases?, normalizePreset?, pcm?, clientSampleRate?, channels?, channelSplit?, options? }
- StreamingController/StreamingSession インタフェース
- BatchResult { provider, text, words?, durationSec?, processingTimeMs? }
- ProviderAdapter { id, supportsStreaming/batch, startStreaming(opts), transcribeFileFromPCM(pcm, opts) }
- StreamServerMessage = StreamTranscriptMessage | NormalizedTranscriptMessage | StreamErrorMessage | StreamSessionMessage
- StreamSessionEndMessage は realtime ログ用に追加

補足: encoding は v1.0 で常に linear16。StreamingSession はサーバ ↔ Adapter 間内部IF。

## 11. 設定／構成管理
- 環境変数例:
```
# backend (Express/WS) の待受ポート（既定: 4100）。
SERVER_PORT=4100
# フロントエンドが backend にアクセスするためのベースURL。
VITE_API_BASE_URL=http://localhost:4100
DEEPGRAM_API_KEY=dg_xxx        # 現行デフォルトで必要なのはこれのみ
ELEVENLABS_API_KEY=xi_xxx      # ElevenLabs を使うにはキーを設定
WHISPER_WS_URL=ws://localhost:8000/v1/audio/transcriptions
WHISPER_HTTP_URL=http://localhost:8000/v1/audio/transcriptions
WHISPER_MODEL=small            # faster-whisper-server へ渡すモデル名
# VOICE_SYSTEM_PROMPT=あなたは日本語で会話する音声アシスタントです。簡潔で自然な日本語で答えてください。
# VOICE_MEMORY_PATH=./voice-memory.txt
# VOICE_HISTORY_MAX_TURNS=12
# 電文が応答しない場合のバッチタイムアウト（ミリ秒）。省略時は 60000。
ELEVENLABS_BATCH_TIMEOUT_MS=60000
# 以下は拡張時に使用（デフォルトでは未使用）
# AZURE_SPEECH_KEY=...
# AZURE_SPEECH_REGION=...
# AWS_ACCESS_KEY_ID=...
# AWS_SECRET_ACCESS_KEY=...
# GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
```
- .env はコミット禁止。
- config.json 例:
```
{
  "audio": { "targetSampleRate": 16000, "targetChannels": 1, "chunkMs": 250 },
  "normalization": { "nfkc": true, "stripPunct": true, "stripSpace": false },
  "storage": { "driver": "jsonl", "path": "./runs/{date}" }, // driver は jsonl または sqlite（{date} は YYYY-MM-DD に展開）
  "providers": ["deepgram", "elevenlabs", "openai", "local_whisper", "whisper_streaming"], // ローカル用途の現行デフォルト（`mock` は任意追加）
  "voice": {
    "memoryPath": "./voice-memory.txt",
    "vad": { "threshold": 0.45, "silenceDurationMs": 500, "prefixPaddingMs": 300 },
    "meetingGate": {
      "enabled": true,
      "minRms": 0.01,
      "noiseAlpha": 0.03,
      "openFactor": 3.0,
      "closeFactor": 1.8,
      "hangoverMs": 250,
      "assistantGuardFactor": 1.5,
      "vad": {
        "enabled": true,
        "mode": 1,
        "frameMs": 20,
        "minSpeechFrames": 2,
        "speechRatio": 0.3
      }
    },
    "tts": {
      "strategy": "segment-buffered",
      "segmentMinChars": 40,
      "segmentMaxChars": 140,
      "retryMax": 2,
      "retryBaseMs": 250
    }
  },
  "jobs": {
    "maxParallel": 4,
    "retentionMs": 600000,
    "persistenceMode": "required",
    "stateDbPath": "./runs/job-state/batch-jobs.sqlite",
    "providerMaxParallel": {
      "openai": 2,
      "deepgram": 3,
      "elevenlabs": 2,
      "local_whisper": 1,
      "whisper_streaming": 2
    },
    "retry": {
      "maxAttempts": 3,
      "baseDelayMs": 1000,
      "maxDelayMs": 10000
    }
  },
  "ws": {
    "maxPcmQueueBytes": 5242880,
    "overflowGraceMs": 500, // バックログ超過時の猶予
    "keepaliveMs": 30000,
    "maxMissedPongs": 2,
    "meeting": {
      "maxPcmQueueBytes": 10485760,
      "overflowGraceMs": 1000
    },
    "preview": {
      "maxUploadBytes": 125829120
    },
    "replay": {
      "minDurationMs": 100,
      "maxUploadBytes": 209715200
    },
    "compare": { "backlogSoft": 8, "backlogHard": 32, "maxDropMs": 1000 } // Realtime比較用バックログ制御
  },
  "providerLimits": {
    "batchMaxBytes": { "openai": 26214400, "deepgram": 52428800 }
  }
}
```
- 日本語CERでプロバイダが空白を挿入してしまうケースを吸収したい場合は、評価マニフェスト（ref_json）側で `normalization.stripSpace=true` を指定してください（ジョブ単位で切替可能）。
- 音声会話の事前記憶は `VOICE_MEMORY_PATH` または `voice.memoryPath` にテキストファイルを指定し、system prompt に追記してセッション開始時に読み込まれます。
- 音声会話の TTS 安定化は `config.voice.tts` でセグメント分割/再試行を調整可能。`strategy: "segment-buffered"` はストリーム断を吸収する代わりに TTFB が伸びます。
- `storage.path` は `{date}` プレースホルダを含められ、`YYYY-MM-DD` に展開される（例: `./runs/{date}`）。展開はサーバ起動時に行われるため、日付跨ぎで切り替えるには再起動が必要。
- `jobs.stateDbPath` はバッチ進捗DBの固定パス（既定: `./runs/job-state/batch-jobs.sqlite`）。`{date}` は使用できない。
- `jobs.persistenceMode` は `required` 固定。進捗DBの初期化に失敗した場合はサーバ起動を停止する。
- 旧 `runs/<date>/batch-jobs.sqlite` を移行する場合は、起動前に `pnpm migrate:job-state -- --from <legacy-db> --to ./runs/job-state/batch-jobs.sqlite` を実行する（起動時の自動シードは行わない）。
- `providerLimits.batchMaxBytes` はプロバイダごとのアップロード上限（バイト）を上書きできる。
- `jobs.maxParallel` はバッチ全体の同時実行上限（CPUコア数と小さい方が実効値）を指定する。
- `jobs.providerMaxParallel` はサーバ内で実行中の**全ジョブ合算**でのプロバイダ別同時実行上限を指定する（外部APIの429回避に有効）。
- `jobs.retry` は batch の一時障害（timeout/429/5xx 等）に対する唯一のリトライ設定（アダプタ側の独自再試行は行わない）。
- バッチジョブの進捗メタデータは `jobs.stateDbPath` に永続化され、プロセス再起動時に `queued/running` のタスクを自動再開する。
- `ws.keepaliveMs` / `ws.maxMissedPongs` は WS の疎通監視設定。
- `ws.overflowGraceMs` はリアルタイム PCM キュー超過時の猶予時間。
- `ws.meeting.*` は meetingMode のみ適用される上書き設定。
- `ws.preview.maxUploadBytes` / `ws.replay.maxUploadBytes` は preview/replay のアップロード上限（バイト）。
- `ws.replay.minDurationMs` は内部再生の最低再生時間（短すぎる音源の誤検知防止）。
- `providerHealth.refreshMs` は `/api/providers` のヘルスチェック再計算間隔（ms）を調整する。`providerHealth.failureThreshold` と `providerHealth.cooldownMs` は batch 実行中の連続失敗を一時的な unavailable に反映するしきい値/クールダウンで、障害中プロバイダへの再投入を抑制する。
- `/api/providers` のレスポンスには `supportsDictionaryPhrases` / `supportsPunctuationPolicy` / `supportsContextPhrases` のようなフラグも含まれるため、クライアントはプロバイダごとの機能差を UI 表示や辞書・句読点コントロールの有効/無効に反映できます。

## 12. 音声処理ポリシー
- Realtime: PCMフレーム（推奨）または webm/opus（互換）。サーバで PCM S16LE（provider 要求に応じて 16k/24k）mono に統一。
- Batch: wav/mp3/mp4。サーバで PCM S16LE 16k mono に統一（公平比較のベースライン）。
- チャンク: 250ms（config.audio.chunkMs）。
- VAD: オプション、公平比較では OFF をデフォルト。
- Voice会話の server VAD は `config.voice.vad` で閾値/無音長/プレフィックスを調整可能（Realtime比較には影響しない）。
- Meet モードの会議音声ゲートは `config.voice.meetingGate` で調整可能（VAD＋通知音ガードで誤反応を抑制）。`meetingGate.vad.mode` を上げるほど厳しめになります。

## 13. 評価設計
- 正規化（基本）: NFKC → 句読点除去。WER の単語境界を壊さないため、空白は保持（`stripSpace=false`）を基本とする。
- 日本語評価（CER）で、プロバイダが挿入した空白を無視したい場合は `stripSpace=true` をジョブ（manifest）側で指定する。
- 指標: CER, WER, RTF=(processingTimeMs/1000)/durationSec。
  - Batch: `processingTimeMs` はサーバ計測（adapter 呼び出し→復帰）。`latencyMs` は互換目的で `processingTimeMs` と同値。
  - Realtime: `latencyMs` は `Date.now() - originCaptureTs`（`originCaptureTs` は PCM フレームの `captureTs`。互換モード等で `captureTs` が無い場合はサーバ受信時刻ベース）。
- 出力: 平均/p50/p95 を集計し UI と CSV/JSON で可視化。再現性確保。

## 14. UI仕様（要点）
- Realtime: プロバイダ選択（現行は Deepgram、ElevenLabs、OpenAI、Whisper Streaming。local_whisper は Batch のみ対応）、言語、VAD・句読点ポリシー・辞書トグル、Start/Stop、部分/確定表示、レイテンシ表示、Chrome/Edge 推奨文言。入力ソースはマイクと「内部ファイル再生」のトグルになっており、ファイル選択時は `/api/realtime/replay` で音声をサーバに送り、返却された `sessionId` を使って `/ws/replay` を開くことで mic-less の比較が可能です。
- Voice: 音声会話（STT/LLM/TTS）を UI から開始/停止、プリセット切替、barge-in 対応。Meet モード用のルーティングとモニタ再生を提供。LLM は Responses API を使用し、Web検索を有効化すると引用リンクを表示。
- Batch Evaluate: ファイルドロップ、参照マニフェスト、並列度（options.parallel）、VAD/句読点/辞書入力、Run、進捗バー、結果テーブル、CSV/JSON DL。
- Results: 最新ジョブの p50/p95 要約と per-file テーブル（provider/cer/wer/rtf/latency）、簡易チャート（p50/p95）を表示。高度な比較グラフは拡張範囲。
- 操作: マイク許可必須、ストリーミング中は Stop 常設、プロバイダ切替時はセッションを安全終了。

## 15. エラーハンドリング・ロギング
- UI: トースト、type:'error' を検出し表示。
- Server: 構造化ログ（time, level, event, provider, jobId, file, latencyMs, rtf, error）。

## 16. セキュリティ
- API鍵の平文コミット禁止、環境変数で管理。
- 送信先ドメイン制限（許可リストのみ）。
- ローカル保存はプロジェクト配下。アップロードパスはサニタイズ（.. 禁止）。

## 17. テスト計画
- ユニット: 正規化関数、CER/WER/RTF、小規模期待値、Adapterモック、TranscriptionOptionsパース。
- 統合: Realtime往復（AudioWorklet→WS(PCMフレーム)→Adapter(Mock)→UI）、StreamServerMessage transcript/error 双方処理、バッチ10件程度、マニフェストと files[] マッピング検証。
- 性能: 250ms チャンクドロップ無し、100×30s で RTF ≤1.0。
- 受け入れ: 成功基準(0.3)を手順書で検証、プロバイダ切替/辞書/正規化差分が UI で比較可能。

## 18. ディレクトリ構成（推奨）
```
stt-compare-local/
  client/
    dist/         # UI build output (server serves if present)
  public/         # fallback static assets (client/dist が無い場合)
  src/
    server.ts
    types.ts
    adapters/
    scoring/
    jobs/
    storage/
  config.json
  .env.example
  README.md
```

## 19. 実装要点（抜粋）
- FFmpeg ブリッジ: `ffmpeg -f webm -i pipe:0 -ac 1 -ar 16000 -f s16le pipe:1` を用い、WSバイナリ→stdin、stdout→StreamingController.sendAudio。
- ストリーミングLC: Start→WS→config→PCMフレーム送信、サーバで Adapter.startStreaming、Stop で controller.end、エラー時は StreamErrorMessage を送信。
- スコア計算: CER/ WER（編集距離）、RTF=(processingTimeMs/1000)/durationSec、集計は平均/p50/p95。

## 20. 拡張計画（例）
- 辞書／ブースト語UI強化、話者分離、ノイズ処理、可視化強化、1ジョブで複数プロバイダ同時評価。

## 21. リスクと軽減策
- API変更→Adapterレイヤでバージョン分離/設定化。
- FFmpeg非導入→初回起動チェック/導入ガイド。
- マシン性能不足→チャンク/並列度制御、ローカルASR選択。
- 鍵漏えい→.env除外、保護、使用後破棄。
- ブラウザ非対応→対象明記、非対応は Batch 案内。

## 22. 受入基準（詳細）
- リアルタイム: 部分→確定が表示、平均レイテンシ<800ms(p95<1.5s) を UI で確認。
- バッチ: 10件以上で CER/WER/RTF 算出、CSV/JSON が仕様通り、同条件で再評価可能。
- 公平性: 16k/mono/PCM投入（ログ記録）、公平比較では VAD OFF。
- 安定性: 連続30分ストリーミングでハングなし。

## 23. 開発・ビルド・実行（標準）
- 要件: Node v20+, FFmpeg。
- セットアップ:
```
pnpm i
pnpm --filter stt-comparator-client i
cp .env.example .env

# 開発（UI: http://localhost:5173 / backend: http://localhost:4100）
pnpm dev
open http://localhost:5173

# 配信（ビルド済みUIを backend が配信: http://localhost:4100）
pnpm build
pnpm start
open http://localhost:4100
```
- スクリプト: `pnpm start`, `pnpm test`, `pnpm build`。デスクトップラッパーは Tauri/Electron で同梱可。

## 24. 付録A：UIワイヤー（簡易）
```
[Realtime]
Provider: [deepgram] Language: [ja-JP] [Dict…] [Start] [Stop]
... こんに | ... こんにちは | ■ こんにちは世界
latency: 350ms (interim), 900ms (final) | session: 00:02:31

[Batch Evaluate]
[ Drop files here ] [Ref Manifest JSON] [Parallel: 4] [Run]
# file prov dur cer wer rtf p50lat p95lat
1 a.wav deepgram 12.3 .078 .132 .34 580ms 980ms
[Export CSV] [Export JSON]
```

## 25. 付録B：評価マニフェスト仕様（JSON Schema イメージ）
```
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["version", "language", "items"],
  "properties": {
    "version": { "type": "integer", "minimum": 1 },
    "language": { "type": "string" },
    "items": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["audio", "ref"],
        "properties": {
          "audio": { "type": "string" },
          "ref": { "type": "string" },
          "meta": { "type": "object" }
        }
      }
    },
    "normalization": {
      "type": "object",
      "properties": {
        "nfkc": { "type": "boolean" },
        "stripPunct": { "type": "boolean" },
        "stripSpace": { "type": "boolean" }
      }
    }
  }
}
```

## 26. 付録C：Adapter 実装ガイド（概要）
- startStreaming(opts): encoding は 'linear16' 固定。ベンダ仕様で接続し PartialTranscript に正規化。sendAudio の引数は PCM 生バイト。
- transcribeFileFromPCM(pcm, opts): 必要なら一時ファイル/アップロード。processingTimeMs を計測して返却。
- local_whisper: Python版 Whisper へのローカル接続。v1 では Batch のみ対応（Streaming 非対応）。モデル/デバイス要件は README に明記。
- nvidia_riva: ローカル常駐サーバへの接続。GPU要件は README に明記。

## 27. 付録D：サンプル正規化ロジック
- 共通（基本）: NFKC → 句読点除去（`stripSpace=false` で空白保持）
- 日本語（CER）: 必要に応じて空白除去（`stripSpace=true`）
- 英語系（WER）: 空白保持が前提（`stripSpace=false`）
