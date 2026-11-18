/** 序号14 跨模块数据契约：拓扑运行数据、节点绑定、降级状态清单。 */

export type NodeBinding = {
  node: string
  module?: string
  config_id?: number
  config_name?: string
  binding_key?: string
  status?: string
  available?: boolean
  pinned?: boolean
  found: boolean
  reason: string
  detail: string
  candidates: NodeBindingCandidate[]
}

export type NodeBindingCandidate = {
  module: string
  config_id: number
  config_name: string
  binding_key: string
  status: string
  available: boolean
}

export type TopologyBindingResult = {
  ok: boolean
  topology: string
  chain: string[]
  nodes: NodeBinding[]
  ambiguous: string[]
  missing: string[]
  invalid: string[]
  unavailable: { node: string; status: string }[]
  signature: string
  run_version: number
  error?: string
}

export type TopologyDeployResult = {
  ok: boolean
  topology: string
  published: boolean
  idempotent?: boolean
  version?: number
  signature?: string
  nodes: NodeBinding[]
  bindings?: Record<string, string>
  blocking_nodes?: string[]
  reasons?: string[]
  error?: string
  message?: string
}

export type SourceMapping = {
  node: string
  module: string
  config_id: number
  source: string
  binding_key: string
}

export type TopologyRuntime = {
  ok: boolean
  topology: string
  published: boolean
  empty: boolean
  empty_reason?: string
  version?: number
  signature?: string
  chain?: string[]
  bindings?: Record<string, string>
  nodes?: NodeBinding[]
  source_mappings: SourceMapping[]
  error?: string
}

export type DegradationEntry = {
  node: string
  source: string
  binding_key: string
  throughput_config_id: number
  throughput_config_name: string
  max_qps: number
  degraded_qps: number
  strategy: string
}

export type DegradationRefusal = {
  node: string
  source: string
  binding_key: string
  reason: string
}

export type DegradationManifest = {
  topology: string
  manifest_version: number
  source_version: number
  source_signature: string
  signature: string
  effect: string
  entries: DegradationEntry[]
  created_at: number
}

export type DegradationResult = {
  ok: boolean
  status: 'ok' | 'reused' | 'refused' | 'empty' | 'missing_topology'
  topology: string
  source_version?: number
  manifest_version?: number
  manifest: DegradationManifest | null
  entries: DegradationEntry[]
  refusals?: DegradationRefusal[]
  similar_names?: string[]
  published?: boolean
  message?: string
  error?: string
}
