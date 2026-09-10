import type { SessionSnapshot } from "./session-store.js";
export type ModelAttachment = {
  blob_id: string;
  media_type: string;
  name?: string;
};

export type ModelMessage = {
  role: "user" | "assistant" | "system" | "tool";
  content: string;
  attachments?: ModelAttachment[];
  reasoning?: string;
  tool_calls?: Array<{ id: string; name: string; arguments: string }>;
  tool_call_id?: string;
};

export type ToolDefinition = {
  name: string;
  description: string;
  input_schema: Record<string, unknown>;
  risk_level?: "low" | "medium" | "high";
  execution?: Record<string, unknown>;
};

export type ModelRequest = {
  run_id: string;
  model: string;
  messages: ModelMessage[];
  tools: ToolDefinition[];
  max_output_tokens?: number;
};

export type BrowserModelConfig = {
  protocol: "responses" | "messages";
  baseUrl: string;
  model: string;
  apiKey: string;
  enableWebSearch: boolean;
  organization?: string;
  project?: string;
  anthropicVersion?: string;
};

export type WorkspaceDebugEntry = {
  path: string;
  name: string;
  kind: "file" | "directory";
  size: number;
  revision?: string;
};

export type WorkspaceDebugSnapshot = {
  root: string;
  descriptor: {
    identity: string;
    kind: string;
    version: string;
    persistent: boolean;
    shared: boolean;
  };
  capabilities: {
    readable: boolean;
    writable: boolean;
    directories: boolean;
    range_read: boolean;
    append: boolean;
    atomic_rename: boolean;
  };
  entries: WorkspaceDebugEntry[];
  truncated: boolean;
};

export type WorkspaceDownload = {
  path: string;
  name: string;
  size: number;
  mediaType: string;
  bytes: ArrayBuffer;
};

export type SkillActivation =
  | { type: "explicit_only" }
  | { type: "profile_default" }
  | { type: "routable"; hints: string[] };

export type SkillDescriptor = {
  skill_id: string;
  version: string;
  description: string;
  digest: string;
  store_identity: string;
  activation: SkillActivation;
};

export type LockedSkill = {
  skill_id: string;
  version: string;
  digest: string;
  store_identity: string;
};

export type SkillLock = {
  skills: LockedSkill[];
  compiler_version: number;
  compiled_instruction_digest: string;
};

export type ResolvedSkill = {
  locked: LockedSkill;
  instruction: string;
  requested_tools: string[];
  activation_reason: string;
};

export type ResolvedSkillSet = {
  resolved_skills: ResolvedSkill[];
  compiled: {
    instruction: string;
    skill_lock: SkillLock;
  };
};

export type SkillInspection = {
  root: string;
  packages: SkillDescriptor[];
};

export type SkillInstallFile = {
  path: string;
  bytes: ArrayBuffer;
};

export type SkillInstallResult = {
  status: "installed" | "already_installed";
  descriptor: SkillDescriptor;
};

export type ToolCall = {
  call_id: string;
  name: string;
  arguments: unknown;
  public_arguments?: unknown;
  risk_level?: "low" | "medium" | "high";
  execution?: Record<string, unknown>;
};

export type PortableToolError = {
  code: string;
  message: string;
  category:
    | "invalid_request"
    | "not_found"
    | "permission_denied"
    | "conflict"
    | "resource_exhausted"
    | "timeout"
    | "cancelled"
    | "unavailable"
    | "internal"
    | "unknown";
  retryable: boolean;
  retry_after_ms?: number;
};

export type ToolInvocationResult =
  | { type: "completed"; content: string }
  | { type: "failed"; error: PortableToolError };

export type AgentEffect = {
  effect_id: string;
  type: string;
  run_id?: string;
  purpose?: "agent_step" | "final_summary" | "session_title";
  approval_id?: string;
  request?: ModelRequest;
  calls?: Array<{ call_id: string; name: string; arguments: unknown }>;
  call?: ToolCall;
  timeout_ms?: number;
};

export type AgentEvent = (
  | { type: "reasoning_started" | "reasoning_completed"; redacted: boolean }
  | { type: "output_delta"; channel: "assistant_text" | "assistant_reasoning"; delta: string }
  | { type: "usage_updated"; usage: { input_tokens: number; output_tokens: number; total_tokens: number; [key: string]: unknown } }
  | { type: "tool_set_updated"; revision: number; digest: string; tools: string[]; dynamic_tools: string[] }
  | { type: "tool_call_started"; call_id: string; name: string }
  | { type: "tool_call_arguments_delta"; call_id: string; delta: string }
  | { type: "approval_requested"; approval_id: string; call_id: string; tool_name: string; risk_level: "low" | "medium" | "high"; arguments: unknown }
  | { type: "approval_resolved"; approval_id: string; call_id: string; resolution: { decision: "allow-once" | "deny"; reason?: string } }
  | { type: "tool_execution_started"; call_id: string; arguments: unknown }
  | { type: "tool_execution_completed"; call_id: string; output: string }
  | { type: "tool_execution_failed"; call_id: string; code: string; message: string; category: PortableToolError["category"]; retryable: boolean; retry_after_ms?: number | null }
  | { type: "cancelled" }
  | { type: "completed"; finish_reason: string }
  | { type: "failed"; code: string; message: string; retryable: boolean }
);

export type AgentTransition = {
  protocol_version: number;
  events: AgentEvent[];
  effects: AgentEffect[];
  outcome: {
    status: "running" | "suspended" | "complete" | "failed" | "cancelled";
    [key: string]: unknown;
  };
  state: {
    messages?: ModelMessage[];
    skill_lock?: SkillLock;
    [key: string]: unknown;
  };
};

export type AgentRunConfig = {
  maxSteps?: number;
  maxOutputTokens?: number;
  modelTimeoutMs?: number;
  toolTimeoutMs?: number;
  approvalPolicy?: number;
  allowedTools?: string[];
};

export type UiToWorker =
  | { type: "initialize"; namespace: string; config: AgentRunConfig }
  | { type: "session"; requestId: string; operation: "list" | "create" | "get" | "update_title"; sessionId?: string; title?: string; expectedRevision?: number }
  | {
      type: "start";
      requestId: string;
      sessionId: string;
      input: string;
      attachments: ModelAttachment[];
      modelConfig: BrowserModelConfig;
    }
  | {
      type: "restore";
      requestId: string;
      sessionId: string;
      runId: string;
      modelConfig: BrowserModelConfig;
    }
  | { type: "cancel"; requestId: string }
  | {
      type: "resolve_approval";
      requestId: string;
      runId: string;
      approvalId: string;
      decision: "allow-once" | "deny";
      reason?: string;
    }
  | {
      type: "generate_title";
      requestId: string;
      sessionId: string;
      firstMessageId: string;
      expectedRevision: number;
      sourceText: string;
      modelConfig: BrowserModelConfig;
    }
  | {
      type: "inspect_workspace";
      requestId: string;
      path?: string;
    }
  | { type: "inspect_skills"; requestId: string }
  | { type: "install_skill"; requestId: string; files: SkillInstallFile[] }
  | { type: "download_workspace"; requestId: string; path: string };

export type WorkerToUi =
  | { type: "approval_ack"; requestId: string }
  | { type: "session_result"; requestId: string; result: SessionSnapshot | SessionSnapshot[] }
  | {
      type: "ready";
      reducerProtocolVersion: number;
      checkpointSchemaVersion: number;
      titleProtocolVersion: number;
      skillCompilerVersion: number;
      skillProvider: "opfs";
      supportedModelProtocols: Array<BrowserModelConfig["protocol"]>;
    }
  | { type: "run_started"; requestId: string; runId: string; restored: boolean }
  | {
      type: "transition";
      requestId: string;
      runId: string;
      transition: AgentTransition;
    }
  | {
      type: "title_generated";
      requestId: string;
      title: string;
      expectedRevision: number;
    }
  | {
      type: "workspace_inspected";
      requestId: string;
      result:
        | { type: "snapshot"; snapshot: WorkspaceDebugSnapshot }
        | { type: "file"; path: string; content: string };
    }
  | { type: "skills_inspected"; requestId: string; result: SkillInspection }
  | { type: "skill_installed"; requestId: string; result: SkillInstallResult }
  | { type: "workspace_downloaded"; requestId: string; result: WorkspaceDownload }
  | { type: "error"; requestId?: string; code: string; message: string };

export type RunWorkerMessage = Extract<
  WorkerToUi,
  { type: "run_started" | "transition" }
>;
