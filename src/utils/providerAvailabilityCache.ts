import type { AppConfig } from '../config.js';
import type { ProviderId } from '../types.js';
import type { ProviderAvailability } from './providerStatus.js';
import { computeProviderAvailability } from './providerStatus.js';

interface CacheEntry {
  timestamp: number;
  value: ProviderAvailability[];
}

interface RuntimeProviderHealth {
  consecutiveFailures: number;
  lastFailureAtMs: number;
  blockedUntilMs: number;
  lastReason?: string;
}

const DEFAULT_FAILURE_THRESHOLD = 3;
const DEFAULT_COOLDOWN_MS = 30_000;

export class ProviderAvailabilityCache {
  #config: AppConfig;
  #refreshMs: number;
  #cache: CacheEntry | null = null;
  #inflight: Promise<ProviderAvailability[]> | null = null;
  #runtimeHealth = new Map<ProviderId, RuntimeProviderHealth>();

  constructor(config: AppConfig, refreshMs = 5_000) {
    this.#config = config;
    this.#refreshMs = Math.max(1, refreshMs);
  }

  updateConfig(config: AppConfig, refreshMs?: number): void {
    this.#config = config;
    if (refreshMs !== undefined) {
      this.#refreshMs = Math.max(1, refreshMs);
    }
    this.#cache = null;
    this.#inflight = null;
    this.#runtimeHealth.clear();
  }

  recordProviderSuccess(provider: ProviderId): void {
    this.#runtimeHealth.delete(provider);
  }

  recordProviderFailure(provider: ProviderId, reason?: string): void {
    const now = Date.now();
    const cooldownMs = this.getCooldownMs();
    const threshold = this.getFailureThreshold();
    const previous = this.#runtimeHealth.get(provider);
    const baseCount =
      previous && now - previous.lastFailureAtMs <= cooldownMs
        ? previous.consecutiveFailures
        : 0;
    const consecutiveFailures = baseCount + 1;
    const blockedUntilMs = consecutiveFailures >= threshold ? now + cooldownMs : 0;
    this.#runtimeHealth.set(provider, {
      consecutiveFailures,
      lastFailureAtMs: now,
      blockedUntilMs,
      lastReason: reason,
    });
  }

  async get(forceRefresh = false): Promise<ProviderAvailability[]> {
    const now = Date.now();
    if (!forceRefresh && this.#cache && now - this.#cache.timestamp < this.#refreshMs) {
      return this.overlayRuntimeHealth(this.#cache.value, now);
    }
    if (this.#inflight) {
      const value = await this.#inflight;
      return this.overlayRuntimeHealth(value, Date.now());
    }
    this.#inflight = computeProviderAvailability(this.#config)
      .then((value) => {
        this.#cache = { value, timestamp: Date.now() };
        this.#inflight = null;
        return value;
      })
      .catch((error) => {
        this.#inflight = null;
        throw error;
      });
    const value = await this.#inflight;
    return this.overlayRuntimeHealth(value, Date.now());
  }

  async refresh(): Promise<ProviderAvailability[]> {
    return this.get(true);
  }

  private getFailureThreshold(): number {
    const configured = this.#config.providerHealth?.failureThreshold;
    if (!configured || !Number.isFinite(configured)) {
      return DEFAULT_FAILURE_THRESHOLD;
    }
    return Math.max(1, Math.floor(configured));
  }

  private getCooldownMs(): number {
    const configured = this.#config.providerHealth?.cooldownMs;
    if (!configured || !Number.isFinite(configured)) {
      return DEFAULT_COOLDOWN_MS;
    }
    return Math.max(1, Math.floor(configured));
  }

  private runtimeState(provider: ProviderId, now: number): RuntimeProviderHealth | null {
    const state = this.#runtimeHealth.get(provider);
    if (!state) return null;

    const cooldownMs = this.getCooldownMs();
    const isBlocked = state.blockedUntilMs > now;
    if (isBlocked) {
      return state;
    }

    if (state.blockedUntilMs > 0 || now - state.lastFailureAtMs > cooldownMs) {
      this.#runtimeHealth.delete(provider);
      return null;
    }

    return state;
  }

  private overlayRuntimeHealth(
    providers: ProviderAvailability[],
    now: number
  ): ProviderAvailability[] {
    return providers.map((provider) => {
      const state = this.runtimeState(provider.id, now);
      if (!state || !provider.available || state.blockedUntilMs <= now) {
        return { ...provider };
      }

      const retryAt = new Date(state.blockedUntilMs).toISOString();
      const failureReason = state.lastReason?.trim();
      const reasonSuffix = failureReason ? `; last error: ${failureReason}` : '';
      return {
        ...provider,
        available: false,
        reason:
          `temporarily unavailable after ${state.consecutiveFailures}` +
          ` consecutive runtime failures; retry after ${retryAt}${reasonSuffix}`,
      };
    });
  }
}
