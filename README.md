# STT Comparator
[![CI](https://github.com/Alivenet-A-Hikkoshiguide/STT-Comparator/actions/workflows/ci.yml/badge.svg)](https://github.com/Alivenet-A-Hikkoshiguide/STT-Comparator/actions/workflows/ci.yml)

ローカル環境で複数の音声認識（ASR）プロバイダを同一条件で比較・計測するミニアプリです。リアルタイム（マイク入力）とファイル一括バッチを共通UIから操作し、CER/WER/RTF やレイテンシを収集・可視化する v1.0 スペックに基づいて設計しています。

## 技術スタック

- **Server**: Node.js 20 + Express + WebSocket（`src/` 直下, 既定ポート 4100）
- **UI**: Vite + React + TypeScript（`client/`、Results タブにプロバイダフィルタと p50/p95 の簡易チャート、Realtime にレイテンシ p50/p95 と履歴テーブル）
- **Audio**: FFmpeg（`@ffmpeg-installer/ffmpeg`）で webm/opus → PCM (16k mono linear16)。アップロード音源もサーバ側で必ず 16k mono PCM WAV に正規化し、デコードエラーや極端に短い/壊れたファイルは **preview/replay では 422**（`AUDIO_UNSUPPORTED_FORMAT` など）で拒否します。batch では投入時に音声ファイル種別（拡張子/MIME）を検証し、非音声が混在していれば 400 で拒否、実行中のデコード失敗はファイル単位で失敗として記録します。必要に応じて strict decode 失敗時に degraded 変換へフォールバックします。
- **Scoring**: 独自 CER/WER/RTF 実装、正規化プリセット
- **Storage**: JSONL 永続化（SQLite ドライバも実装済み; storage.driver で切替）
- **Voice Agent（音声会話）**: STT/TTS（ElevenLabs / OpenAI）+ OpenAI（LLM）。割り込み（barge-in）対応。

## 主要ディレクトリ

```
├── src/                  # ローカルサーバ（Express/WS）
│   ├── adapters/         # Provider Adapter 実装（deepgram / elevenlabs / local_whisper / whisper_streaming / openai。mock は任意のスタブ）
│   ├── jobs/             # バッチ実行・スコアリング
│   ├── scoring/          # CER/WER/RTF と正規化
│   ├── storage/          # JSONL/SQLite ドライバ, CSVエクスポータ
│   ├── utils/            # FFmpeg ブリッジ, マニフェストパーサ
│   ├── ws/               # Realtime WebSocket handler
│   └── server.ts         # Express + WS エントリポイント
├── client/               # Web UI (React), build output: client/dist (served by server)
│   ├── src/App.tsx       # Realtime + Batch UI ワイヤー
│   └── vite.config.ts    # 開発/ビルド設定（API Proxy）
├── public/               # static assets (client/dist が無い場合のフォールバック)
├── sample-data/          # manifest 雛形
├── config.json           # v1.0 仕様のアプリ設定
└── .env.example          # API鍵やポートのテンプレート
```

## セットアップ

1. FFmpeg がローカルにインストールされていることを確認してください。
2. 依存関係をインストール:
   ```bash
   pnpm install
   pnpm --filter stt-comparator-client install
   ```
3. 環境変数テンプレートを複製して API キー等を設定（`DEEPGRAM_API_KEY` が未設定の場合、UI で Deepgram は「unavailable」と表示され選択不可）:
 ```bash
  cp .env.example .env
  ```
  - Deepgram を有効化するには `.env` に `DEEPGRAM_API_KEY` を入れてサーバを再起動するだけでOK。既定ポートは 4100（`SERVER_PORT` で変更可能、フロントは `VITE_API_BASE_URL` と Vite proxy がそれに追従します）。
4. `config.json` の `storage.path` や `providers` を利用状況に合わせて調整（デフォルトは `deepgram`, `elevenlabs`, `local_whisper`, `whisper_streaming`, `openai`。手元検証用に `mock` を追加することもできます）。`storage.path` は `{date}` プレースホルダを含められ、`YYYY-MM-DD` に展開されます（例: `./runs/{date}`）。展開はサーバ起動時に行われるため、日付跨ぎでフォルダを切り替えたい場合は再起動してください。`jobs.stateDbPath` は日付展開しない固定パス（既定: `./runs/job-state/batch-jobs.sqlite`）を指定してください。`jobs.persistenceMode` は `required` 固定で、進捗DBが初期化できない場合は起動を停止します。`elevenlabs` を使うには `.env` に `ELEVENLABS_API_KEY` を設定してください。
  - `ELEVENLABS_BATCH_TIMEOUT_MS`: 初期リクエストのタイムアウト（既定 60000ms）。
  - 旧 `runs/<date>/batch-jobs.sqlite` から進捗DBを移行する場合は、サーバ起動前に `pnpm migrate:job-state -- --from <legacy-db> --to ./runs/job-state/batch-jobs.sqlite` を実行してください（起動時の自動シードは行いません）。
必要に応じて `ALLOWED_ORIGINS` をカンマ区切りで設定し、CORS/CSP/WS の許可先を絞り込めます。保存データの肥大化を防ぐため `storage.retentionDays` と `storage.maxRows` で保持期間と件数上限を設定できます（デフォルト: 30日 / 100,000件）。`ws.keepaliveMs`/`ws.maxMissedPongs` で WS の疎通監視、`ws.meeting` で meetingMode 時のキュー閾値、`ws.replay.minDurationMs` で内部再生の最小再生時間、`ws.preview.maxUploadBytes`/`ws.replay.maxUploadBytes` で preview/replay のアップロード上限、`providerLimits.batchMaxBytes` でプロバイダ毎のバッチ上限を調整できます。バッチの実行安定性は `jobs.maxParallel`（全体上限）、`jobs.providerMaxParallel`（サーバ内全ジョブ合算のプロバイダ別上限）、`jobs.retry`（一時障害リトライ）で調整でき、ジョブ状態は `jobs.persistenceMode='required'` で `jobs.stateDbPath` に永続化されるため、結果保存先を日次ローテーションしてもプロセス再起動後に `queued/running` のジョブを自動再開できます。
  - `providerHealth.refreshMs` で `/api/providers` のヘルスチェック結果のキャッシュ期間（ミリ秒）を調整できます。デフォルト 5000ms により、`whisper_streaming` などのローカル ASR サービスを起動した直後でも UI が利用可能に切り替えられ、必要があれば `/api/admin/reload-config` を呼んで即座に再評価させられます。

### 音声会話（Voice Agent）

UI の「音声会話」タブは、マイク入力 → STT → LLM → TTS → ブラウザ再生、の往復で会話します。話しかけることで返答を割り込めます（barge-in）。スピーカー利用時はエコー混入を避けるためヘッドホン推奨です。

- プリセット切替: UI のドロップダウンで選択（サーバの `GET /api/voice/status` が返す `presets` を表示）
  - 既定（built-in）: `elevenlabs` / `openai_realtime` / `openai` / `deepgram`
  - 変更: `config.json` に `voice.presets`（`id`, `label`, `sttProvider`, `ttsProvider`）と `voice.defaultPresetId`
  - 既定値: `voice.defaultPresetId` → `VOICE_PRESET_ID` → (`VOICE_STT_PROVIDER` + `VOICE_TTS_PROVIDER`) → 最初の利用可能プリセット
- 必須: 選択したプリセットに応じたキー（`GET /api/voice/status` の `missing` / `missingEnv` を参照）
  - `elevenlabs`: `ELEVENLABS_API_KEY`, `ELEVENLABS_TTS_VOICE_ID`, `OPENAI_API_KEY`
  - `openai`: `OPENAI_API_KEY`
  - `openai_realtime`: `OPENAI_API_KEY`
  - `deepgram`: `DEEPGRAM_API_KEY`, `OPENAI_API_KEY`
- ヘルス: `GET /api/voice/status`
- WS: `/ws/voice?lang=ja-JP`
- 事前記憶: `VOICE_MEMORY_PATH` または `config.json` の `voice.memoryPath` にテキストファイルを指定すると、system prompt に追記してセッション開始時に読み込みます。
- VAD調整: `config.json` の `voice.vad` で閾値/無音長/プレフィックスを調整（音声会話のみ影響、Realtime比較には影響しません）。
- TTS安定化: `config.json` の `voice.tts` でセグメント分割 + 再試行を調整可能（既定は `strategy: "segment-buffered"` で途切れを抑止）。
  - `segmentMinChars` / `segmentMaxChars`（1セグメントの文字数）
  - `retryMax` / `retryBaseMs`（再試行回数と待機時間）
- 参考: `voice_assistant_audio_start` に `llmMs` / `ttsTtfbMs` が含まれ、UI に表示されます（体感遅延の内訳確認に利用）。
- 日本語品質: `lang` が `ja-*` で `ELEVENLABS_TTS_MODEL_ID` 未設定の場合は `eleven_multilingual_v2` を自動適用（アカウント/モデル非対応時は自動で無指定にフォールバック）。
- 速度調整: `ELEVENLABS_TTS_OPTIMIZE_STREAMING_LATENCY`（例: `3`）で ElevenLabs の初動レイテンシをチューニングできます。
  - OpenAI LLM（pipeline）: Responses API を使用。`OPENAI_RESPONSES_MODEL`（既定 `gpt-5.2`）でモデル選択、`OPENAI_WEB_SEARCH_ENABLED` を有効にすると最新情報検索 + 引用表示が可能です。
  - Web検索制御: `OPENAI_WEB_SEARCH_ALLOWED_DOMAINS`（許可ドメイン）、`OPENAI_WEB_SEARCH_CONTEXT_SIZE`（検索文脈サイズ）、`OPENAI_WEB_SEARCH_EXTERNAL_ACCESS`（外部アクセス許可）
  - OpenAI TTS: `OPENAI_TTS_MODEL`（既定 `gpt-4o-mini-tts`）, `OPENAI_TTS_VOICE`（既定 `alloy`）

#### Meet モード（Web会議の参加者とも会話する）

Google Meet（ブラウザ版）のタブ音声を取り込み、さらに **AI音声＋自分の声** を仮想デバイスへ出力して Meet のマイク入力として使うことで、参加者全員に AI の返答を聞かせられます（ローカル運用向けの恒久構成）。

- 前提: Chrome/Edge 推奨（`setSinkId` 対応が必要）
- macOS 例: BlackHole 等の仮想オーディオデバイスを用意
- 手順（概要）
  - Meet 側でマイク入力を仮想デバイス（例: BlackHole）に変更
  - 本アプリの「音声会話」タブで「Meet に自分＋AI の音声を送る」をON → 出力デバイスに仮想デバイスを選択
  - 他参加者の発話にも反応させたい場合、「Meet タブ音声を取り込む」をON → 開始時の共有ダイアログで **Google Meet のタブ** を選び「タブの音声を共有」をON
  - 会議の誤反応を避けるため、既定で「会議音声は呼びかけ（wake word）必須」を推奨
- 注意
  - 参加者音声がクラウドSTT/LLM/TTSへ送信されるため、利用前に同意・ポリシー確認を推奨
  - ローカル再生（モニタ）をONにする場合はヘッドホン推奨（エコー/回り込み対策）
  - Meet 出力とモニタ出力が同一デバイスの場合、二重再生を避けるためモニタ再生は自動で無効化されます
  - マイク入力と Meet 出力が同一デバイスの場合、ループバックを避けるため Meet への自分の声のミックスは自動で停止されます

### Whisper Streaming（faster-whisper-server, ローカル常駐）

- Docker 1コマンドで常駐サーバを起動できます（CPU 版の例、ポート 8000 固定）:
  ```bash
  docker run --rm -p 8000:8000 fedirz/faster-whisper-server:latest-cpu
  # GPU を使う場合は :latest-cuda イメージを選択
  ```
- .env に以下を追加（`cp .env.example .env` 済みの場合は値を書き換えるだけ）:
  ```bash
  WHISPER_WS_URL=ws://localhost:8000/v1/audio/transcriptions
  WHISPER_HTTP_URL=http://localhost:8000/v1/audio/transcriptions
  WHISPER_MODEL=small   # 任意モデル名
  ```
  - `whisper_streaming` Adapter は faster-whisper-server が WebSocket/HTTP をリッスン可能になるまでヘルスチェック（デフォルト: `http://localhost:8000/health`）をポーリングし、それが完了してからソケット接続または HTTP リクエストを開始します。必要に応じて以下の追加環境変数でポーリング先・タイムアウト・間隔を調整できます（全て省略または 0 にするとヘルスチェックを無効化します）。
    ```bash
    WHISPER_STREAMING_READY_URL=http://localhost:8000/health
    WHISPER_STREAMING_READY_TIMEOUT_MS=90000
    WHISPER_STREAMING_READY_INTERVAL_MS=1000
    ```
- `whisper_streaming` は **Realtime / Batch の両方に対応** します。UI/Batch API から選択すると PCM(16k mono) をローカルサーバへ送信し、partial/final の文字起こしを受信します。
- Deepgram など外部 API と同等にレイテンシ計測・公平性指標（p50/p95）が動作します。サーバ未起動時は `/api/providers` で unavailable 理由が返り、UI では選択不可・警告表示になります。

### OpenAI Realtime / Transcribe（GPT-4o）

- `.env` に `OPENAI_API_KEY` を設定すると Realtime/Bulk の両方で `openai` プロバイダが利用可能になります。キー未設定時は `/api/providers` が unavailable と返し、UI で選択不可になります。
- ストリーミングは OpenAI Realtime API（intent=transcription, model=`gpt-4o-transcribe` 既定）を利用します。API が 24kHz PCM を要求するため、必要に応じてサーバ側で 16kHz → 24kHz にアップサンプリングして送出し、処理時間に含めます。VAD は公平比較のため既定OFF（`enableVad=true` で server_vad を有効化）。
- バッチは Audio Transcriptions API（model=`gpt-4o-transcribe` 既定）に WAV ラップした 16kHz PCM を multipart で送信し、word-level timestamps が返れば UI/CSV に反映されます。
- モデルは環境変数で切替可能: `OPENAI_STREAMING_MODEL`, `OPENAI_BATCH_MODEL`（例: `gpt-4o-mini-transcribe`）。

### ローカル Whisper (Python) を使う

- 依存: Python 3.10–3.13 推奨（本リポでは 3.11 を例示）、`ffmpeg` は既に必須。
- セットアップ例（リポジトリ直下で実行）
  ```bash
  python3.11 -m venv .venv
  source .venv/bin/activate
  pip install --upgrade pip setuptools wheel
  pip install git+https://github.com/openai/whisper.git
  ```
- サーバはデフォルトで `.venv/bin/python3` を優先し、`WHISPER_PYTHON` を指定すれば別 Python を使えます。モデル/デバイスは環境変数で調整できます（例: `WHISPER_MODEL=small`, `WHISPER_DEVICE=cpu`）。
- `local_whisper` は **Batch のみ対応** です（Realtime は非対応）。Realtime を試す場合は Deepgram 等のストリーミング対応プロバイダを選択してください。

## 実行

- サーバ単体（API + WS + static 配信）: `pnpm run dev:server`
- UI 単体（Vite Dev Server, http://localhost:5173）: `pnpm run dev:client`
  - `dev:client` はフロントエンドのみ起動します。API/WS を使う場合は別ターミナルで `pnpm run dev:server` を同時起動してください。
- フルスタック（サーバ + UI 同時）: `pnpm run dev`
- ビルド: `pnpm run build`（サーバをコンパイルし、UI を `client/dist` に出力。Express は `client/dist` があればそれを配信し、無ければ `public/` を配信します。デフォルトで `COREPACK_HOME=.corepack-cache` を使用）
- 本番起動: `pnpm start`

UI ビルド後は `client/dist` のファイルを Express が配信します（`client/dist` が無い場合は `public/` を使用）。開発中は Vite が `/api`・`/ws` へのプロキシを介してサーバと通信します。

## API・WS 概要

- `POST /api/jobs/transcribe` — multipart/form-data で `files[]`, `provider`, `lang`, `ref_json`, `options` を受け取り、UUID ベースの `jobId` を返却。非音声ファイルが含まれる場合は 400（`invalidFiles` を同梱）で拒否。
- `GET /api/jobs/:jobId/status` — 進捗（`total/done/failed/status/providers/errors/warnings`）を返却。`warnings` には `parallel_clamped`（並列制約）、`degraded_audio_detected`（degraded 正規化混在）の通知が入ります。
- `GET /api/jobs/:jobId/summary` — `{ count, cer, wer, rtf, latencyMs, degraded:{count,ratio} }` を返却。`degraded` でフォールバック混在率を確認可能。
- `GET /api/jobs/:jobId/results?format=csv|json` — CER/WER/RTF を含むレコードを JSON/CSV で返却。
- `WS /ws/stream?provider=<id>&lang=<bcp47>` — 接続直後に `StreamingConfigMessage` を送信 → 以降は PCM フレーム（推奨）または webm/opus バイナリ（互換）を送る。
- UI では punctuation policy（none/basic/full）を選択して送信でき、プロバイダ側の句読点挿入挙動を条件として比較可能。
- `GET /api/providers` — 現在のプロバイダ有効/無効状態を返却（Deepgram キー未設定などを UI で案内）。この API は現在のプロバイダヘルスを逐次再評価し `providerHealth.refreshMs` ミリ秒ごとにキャッシュを更新するため、ローカルの `whisper_streaming` サーバを起動した後でも数秒以内に UI に「利用可能」と反映されます。
- `GET /api/realtime/latency?limit=20` — 直近の realtime セッションのレイテンシ集計（avg/p50/p95/min/max, count, provider/lang）を返却。
- `GET /api/realtime/logs/:sessionId` — `session/transcript/normalized/error/session_end` の t と `latencyMs` を含むイベントを時系列で返す。ログは `config.json` の `storage.path` 配下に JSONL 形式で蓄積され（`storage.path` が `{date}` を含む場合は `runs/<date>` に展開）、生成AIなどにそのまま送れる診断シーケンスとして使える。

### Realtime replay helper

`scripts/realtime-replay.ts` はブラウザの AudioWorklet（PCM フレーム）→ `/ws/stream` → Adapter 経路を TypeScript で再現し、音声ファイルを `ffmpeg -re` で PCM16LE に変換して WS に流し込みます。物理マイクが使えない静音環境でも、provider・言語・マニフェストファイルを切り替えながら latency や transcripts を再現性高く検証できます。`pnpm replay:realtime` から起動でき、`--file sample-data/your.wav` や `--manifest sample-data/manifest.example.json` といった引数で対象を指定します。

- **UI 内部再生**: Realtime タブには「入力ソース」トグルが入り、マイクのほか「内部ファイル再生」を選べます。ファイルを選ぶと UI は `/api/realtime/replay` に音声をアップロードし、返却された `sessionId` を使って `/ws/replay` に接続するため、マイクなしで同じストリーミング経路（FFmpeg → Adapter）を検証できます。CLI ヘルパーと併用して再現性を上げるのが推奨です。

- **CLI の主要オプション**
  - `--provider`/`--language`: 送信先と BCP-47
  - `--file`（repeatable）: 単一音源
  - `--manifest`: `audio`/`ref` を持つ manifest（`sample-data/manifest.example.json`）
  - `--enable-interim`, `--normalize-preset`, `--punctuation`, `--context`, `--dictionary`: UI と同じ StreamingConfig の制御
  - `--ffmpeg-path`/`--dry-run`: FFmpeg 実行パスと config の検証

スクリプトは `StreamTranscriptMessage` を受信するたびに `transcript`, `latencyMs`, `isFinal` を出力し、終了後に平均・p95・最大 latency を集計します。Deepgram を使うには `.env` に `DEEPGRAM_API_KEY`、`whisper_streaming` では faster-whisper-server を起動済みで `WHISPER_STREAMING_READY_URL` が通る状態にしておいてください。`mock` でまず挙動を確認したうえで本物プロバイダや `GET /api/realtime/latency?limit=20` との突合を行うと、静音環境でも本番相当の検証が可能です。

詳細なスキーマは仕様書（`src/types.ts`）と README のリンク箇所を参照してください。

## スコアリング / ストレージ

- 正規化: `config.json` や manifest の `normalization` フィールドで NFKC/句読点/空白/小文字化を制御（デフォルトは stripSpace=false）。
- CER/WER/RTF: `src/scoring/metrics.ts`。ユニットテスト（`pnpm test`）で動作確認。
- ストレージ: 既定は JSONL（`storage.path` 配下に `results.jsonl` / `realtime.jsonl` / `realtime-logs.jsonl`）。`storage.driver` を `sqlite` にすれば `results.sqlite`（バッチ結果）/`realtime.sqlite`（レイテンシ）/`realtime-logs.sqlite`（リアルタイムログ）へ分離保存します。

## Adapter 雛形

- `src/adapters/mock.ts` — ローカル確認/テスト用のスタブ。通常は `config.json` の providers に含まれませんが、必要なら手動追加できます。
- `src/adapters/deepgram.ts` — Deepgram 公式 API 連携を実装済み。`.env` に `DEEPGRAM_API_KEY` を設定して有効化。
- `src/adapters/elevenlabs.ts` — ElevenLabs STT（Streaming/Batch）。`.env` に `ELEVENLABS_API_KEY` を設定して有効化。
- `src/adapters/openai.ts` — OpenAI Realtime/Bulk STT（Streaming/Batch）。`.env` に `OPENAI_API_KEY` を設定して有効化。
- `src/adapters/localWhisper.ts` — ローカル Whisper（Batch のみ）。Python/Whisper 環境が必要。
- `src/adapters/whisperStreaming.ts` — ローカル常駐の faster-whisper-server (WS/HTTP) と連携する Streaming/Batch 両対応アダプタ。
- Adapter を増やす際は `src/adapters/index.ts` に登録し、`config.json` の `providers` に ID を追加します。

## サンプル manifest

`sample-data/manifest.example.json` は v1.0 仕様の参照 JSON。UI のバッチエリアや `POST /api/jobs/transcribe` の `ref_json` に流用できます（音声ファイルは同梱していないため、`items[].audio` に対応するファイルを用意してください）。

## テスト

- サーバ: `pnpm test`（Vitest, Node env）
- クライアント: `pnpm --filter stt-comparator-client test`

## CI

- GitHub Actions (`.github/workflows/ci.yml`) で lint → test → build を実行。Node 20 + pnpm 10.12.1 を Corepack で準備し、`COREPACK_HOME` をローカルキャッシュに向けています。

### Corepack キャッシュ権限エラーが出る場合
共有環境で `pnpm build` が `~/.cache/node/corepack` への書き込み権限不足で失敗することがあります。`pnpm run build` は自動で `COREPACK_HOME=.corepack-cache` をセットしますが、明示的に変えたい場合は以下のように上書きできます。
```bash
export COREPACK_HOME="$(pwd)/.corepack-cache"
pnpm run build
```

## 未実装 / TODO

- （必要なら）AWS/GCP/Azure などクラウド実プロバイダ Adapter 実装
- 結果比較の高度な可視化（複数ジョブ横断）
- ジョブの永続キューイング（途中再開, 並列制御）

## ライセンス

MIT
