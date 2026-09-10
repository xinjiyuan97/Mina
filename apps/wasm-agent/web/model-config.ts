import type { BrowserModelConfig } from "@mina/browser-agent";

const SETTINGS_KEY = "mina.browser-agent.model-settings.v1";

type StoredModelConfig = Omit<BrowserModelConfig, "apiKey">;

const defaults: Record<BrowserModelConfig["protocol"], StoredModelConfig> = {
  responses: {
    protocol: "responses",
    baseUrl: "https://api.openai.com/v1/",
    model: "gpt-5.5",
    enableWebSearch: true,
  },
  messages: {
    protocol: "messages",
    baseUrl: "https://api.anthropic.com/v1/",
    model: "claude-sonnet-4-5",
    enableWebSearch: true,
    anthropicVersion: "2023-06-01",
  },
};

export function loadBrowserModelConfig(): BrowserModelConfig {
  const fallback = { ...defaults.responses, apiKey: "" };
  try {
    const source = window.localStorage.getItem(SETTINGS_KEY);
    if (!source) return fallback;
    const parsed = JSON.parse(source) as Partial<StoredModelConfig>;
    if (parsed.protocol !== "responses" && parsed.protocol !== "messages") return fallback;
    const base = defaults[parsed.protocol];
    return {
      ...base,
      ...parsed,
      protocol: parsed.protocol,
      apiKey: "",
    };
  } catch {
    return fallback;
  }
}

export function saveBrowserModelConfig(config: BrowserModelConfig) {
  const { apiKey: _secret, ...stored } = config;
  window.localStorage.setItem(SETTINGS_KEY, JSON.stringify(stored));
}

export function switchBrowserModelProtocol(
  current: BrowserModelConfig,
  protocol: BrowserModelConfig["protocol"],
): BrowserModelConfig {
  if (current.protocol === protocol) return current;
  return { ...defaults[protocol], apiKey: "" };
}

export function isBrowserModelConfigured(config: BrowserModelConfig) {
  return Boolean(config.baseUrl.trim() && config.model.trim() && config.apiKey.trim());
}

export function browserModelLabel(config: BrowserModelConfig) {
  return config.model.trim() || `${config.protocol} · setup required`;
}

export function browserToolNames(config: BrowserModelConfig) {
  const local = ["read", "write", "edit", "list_directory", "javascript_eval"];
  return config.protocol === "responses"
    ? [...local, "generate_image", "edit_image"]
    : local;
}
