import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'
import type {
  DegradationResult,
  TopologyBindingResult,
  TopologyDeployResult,
  TopologyRuntime,
} from '@/data/topology'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

// ---------------------------------------------------------------------------
// 序号14：拓扑节点绑定 / 运行数据 / 降级状态清单
// ---------------------------------------------------------------------------

async function readJson<T>(path: string, init?: RequestInit, fallback = '业务数据读取失败'): Promise<T> {
  const response = await request(path, init)
  if (!response.ok) throw new Error(fallback)
  return (await response.json()) as T
}

/** 拓扑工作台：查看每个节点的实际模块绑定（只读）。 */
export function loadTopologyBinding(entryId: number): Promise<TopologyBindingResult> {
  return readJson<TopologyBindingResult>(
    `/api/task_topology/${entryId}/binding`,
    undefined,
    '节点绑定读取失败',
  )
}

/** 按完整标识校验并发布运行版本；失败时返回逐节点原因。 */
export function deployTopology(entryId: number): Promise<TopologyDeployResult> {
  return readJson<TopologyDeployResult>(
    `/api/task_topology/${entryId}/deploy`,
    { method: 'POST' },
    '拓扑部署失败',
  )
}

/** 拓扑运行数据：已发布版本、节点绑定、源端映射。 */
export function loadTopologyRuntime(topologyName: string): Promise<TopologyRuntime> {
  return readJson<TopologyRuntime>(
    `/api/topology_runtime/${encodeURIComponent(topologyName)}`,
    undefined,
    '拓扑运行数据读取失败',
  )
}

/** 吞吐页面：读取当前降级状态清单。 */
export function loadDegradationManifest(topologyName: string): Promise<DegradationResult> {
  const query = new URLSearchParams({ topology: topologyName }).toString()
  return readJson<DegradationResult>(
    `/api/throughput_mgr/degradation?${query}`,
    undefined,
    '降级状态清单读取失败',
  )
}

/** 吞吐页面：依据拓扑运行数据生成降级清单，歧义时拒绝写入。 */
export function buildDegradationManifest(topologyName: string): Promise<DegradationResult> {
  const query = new URLSearchParams({ topology: topologyName }).toString()
  return readJson<DegradationResult>(
    `/api/throughput_mgr/degradation?${query}`,
    { method: 'POST' },
    '降级状态清单生成失败',
  )
}
