import { useCallback, useEffect, useMemo, useState } from 'react'

import { request } from '@/api/client'

const ENDPOINT = '/api/streamin'
const columns = [
  "源编号",
  "源名称",
  "消息队列",
  "主题名称",
  "分区数量",
  "消费组",
  "起始位点",
  "源状态",
  "最后确认位点"
]
const actions = [
  "启消费",
  "暂停消费",
  "关闭源"
]
const statuses = [
  "草稿",
  "运行中",
  "已暂停",
  "已关闭"
]
const stats = [
  {
    "label": "流数据源总数",
    "value": 0
  },
  {
    "label": "运行中源",
    "value": 0
  },
  {
    "label": "已暂停源",
    "value": 0
  }
]

type Row = Record<string, string | number | null>

type ResumeItem = {
  源编号: string
  消费组: string
  分区: number
  暂停前最后位点: number
  批次检查点位点: number
  最后确认位点: number
  处理结果: string
  失败原因: string | null
}

type ResumeBatch = {
  id: number
  批次数据集ID: number
  幂等键: string
  状态: string
  成功数: number
  失败数: number
  待处理数: number
  成功项: ResumeItem[]
  失败项: ResumeItem[]
  待处理项: ResumeItem[]
  明细: ResumeItem[]
}

export default function StreaminView() {
  const [rows, setRows] = useState<Row[]>([])
  const [total, setTotal] = useState(0)
  const [errorMessage, setErrorMessage] = useState('')
  const [filters, setFilters] = useState<Record<string, string>>({})
  const [selected, setSelected] = useState<number[]>([])
  const [batches, setBatches] = useState<ResumeBatch[]>([])
  const [batchMessage, setBatchMessage] = useState('')
  const filterFields = useMemo(() => columns.slice(0, 3), [])

  const reload = useCallback(async () => {
    setErrorMessage('')
    const query = new URLSearchParams(filters).toString()
    try {
      const response = await request(`${ENDPOINT}?${query}`)
      if (!response.ok) {
        throw new Error('流数据源列表读取失败')
      }
      const payload = await response.json()
      setRows(payload.items ?? [])
      setTotal(payload.total ?? 0)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '流数据源接入列表读取失败')
    }
  }, [filters])

  const reloadBatches = useCallback(async () => {
    try {
      const response = await request(`${ENDPOINT}/resume`)
      if (!response.ok) {
        throw new Error('位点续做数据集读取失败')
      }
      const payload = await response.json()
      setBatches(payload.items ?? [])
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '位点续做数据集读取失败')
    }
  }, [])

  useEffect(() => {
    void reload()
    void reloadBatches()
  }, [reload, reloadBatches])

  function toggleSelect(id: number) {
    setSelected((prev) => prev.includes(id) ? prev.filter((item) => item !== id) : [...prev, id])
  }

  async function runAction(action: string, row: Row) {
    setErrorMessage('')
    try {
      const response = await request(`${ENDPOINT}/${row.id}/actions`, {
        method: 'POST',
        body: JSON.stringify({ action }),
      })
      if (!response.ok) {
        throw new Error('流数据源接入动作未生效，请稍后重试')
      }
      await reload()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '流数据源接入操作失败')
    }
  }

  async function runBatchPause() {
    setErrorMessage('')
    setBatchMessage('')
    if (selected.length === 0) {
      setErrorMessage('请先勾选需要批量暂停的流数据源')
      return
    }
    // 幂等键由勾选集合生成：同一批源重复点击只生效一次，只确认一个 offset。
    const idemKey = `pause-${[...selected].sort((a, b) => a - b).join('-')}`
    try {
      const response = await request(`${ENDPOINT}/batch-pause`, {
        method: 'POST',
        body: JSON.stringify({
          source_ids: selected,
          idem_key: idemKey,
          // 演示失败分区与续做边界：勾选 3 条及以上时让最后一条模拟确认失败。
          fail_simulation: selected.length >= 3 ? [selected[selected.length - 1]] : [],
        }),
      })
      const payload = await response.json()
      if (!response.ok) {
        throw new Error(payload?.detail ?? '批量暂停未生效，请稍后重试')
      }
      const batch = payload.batch
      setBatchMessage(
        `批次 ${batch.批次数据集ID}：成功 ${batch.成功数} 项、失败 ${batch.失败数} 项、待处理 ${batch.待处理数} 项`
        + `${batch.重复提交 ? '（重复提交，沿用首次确认结果）' : ''}`
      )
      await reload()
      await reloadBatches()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '批量暂停操作失败')
    }
  }

  return (
    <section className="page" data-module="streamin">
      <header className="page-head">
        <div>
          <h2>流数据源接入管理</h2>
          <p className="page-desc">维护流数据源的消息队列配置、分区管理与位点控制，为流数据源接入提供可验证的数据工程流程。</p>
        </div>
        <div className="page-actions">
          <button className="btn primary" type="button" onClick={() => setErrorMessage('流数据源登记入口尚未接入审批流')}>
            登记流数据源
          </button>
          <button className="btn" type="button" onClick={() => window.open(`${ENDPOINT}/export`, '_blank')}>
            导出流数据源接入清单
          </button>
        </div>
      </header>

      <div className="stat-row">
        {stats.map((item) => (
          <article key={item.label} className="stat-card">
            <span className="stat-label">{item.label}</span>
            <strong className="stat-value">{item.value}</strong>
          </article>
        ))}
      </div>

      <form className="filter-bar" onSubmit={(event) => { event.preventDefault(); void reload() }}>
        {filterFields.map((field) => (
          <label key={field} className="filter-item">
            <span>{field}</span>
            <input
              value={filters[field] ?? ''}
              placeholder={`按${field}检索`}
              onChange={(event) => setFilters((prev) => ({ ...prev, [field]: event.target.value }))}
            />
          </label>
        ))}
        <button className="btn" type="submit">查询</button>
        <button className="btn ghost" type="button" onClick={() => setFilters({})}>重置条件</button>
      </form>

      <div className="page-actions" style={{ margin: '12px 0' }}>
        <button className="btn primary" type="button" onClick={() => void runBatchPause()}>
          批量暂停（已选 {selected.length} 项）
        </button>
        {batchMessage && <span className="stat-label">{batchMessage}</span>}
      </div>

      <table className="data-table">
        <thead>
          <tr>
            <th>选择</th>
            {columns.map((column) => <th key={column}>{column}</th>)}
            <th>可执行动作</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={String(row.id)}>
              <td>
                <input
                  type="checkbox"
                  checked={selected.includes(Number(row.id))}
                  onChange={() => toggleSelect(Number(row.id))}
                />
              </td>
              {columns.map((column) => <td key={column}>{row[column] ?? '—'}</td>)}
              <td className="row-actions">
                {actions.map((action) => (
                  <button key={action} className="link" type="button" onClick={() => void runAction(action, row)}>
                    {action}
                  </button>
                ))}
              </td>
            </tr>
          ))}
          {rows.length === 0 && (
            <tr>
              <td className="empty-state" colSpan={columns.length + 2}>暂无流数据源接入数据，可先登记流数据源</td>
            </tr>
          )}
        </tbody>
      </table>

      <h3 style={{ margin: '24px 0 8px' }}>位点续做数据集（与流作业指标采集页面共用同一份）</h3>
      <p className="page-desc">
        每个批次只写入一次已确认 offset；中断批次保留失败分区和最后确认位置，源状态与本数据集一起停在未完成边界。
      </p>
      <table className="data-table">
        <thead>
          <tr>
            <th>批次数据集ID</th>
            <th>幂等键</th>
            <th>批次状态</th>
            <th>成功项</th>
            <th>失败项（保留最后确认位置）</th>
            <th>待处理项</th>
          </tr>
        </thead>
        <tbody>
          {batches.map((batch) => (
            <tr key={batch.id}>
              <td>{batch.批次数据集ID}</td>
              <td>{batch.幂等键}</td>
              <td>{batch.状态}</td>
              <td>
                {batch.成功项.map((item) => (
                  <div key={item.源编号}>{item.源编号}：确认位点 {item.最后确认位点}</div>
                ))}
              </td>
              <td>
                {batch.失败项.length === 0 ? '—' : batch.失败项.map((item) => (
                  <div key={item.源编号}>
                    {item.源编号}：分区 {item.分区}，最后确认 {item.最后确认位点}（{item.失败原因 ?? '失败'}）
                  </div>
                ))}
              </td>
              <td>
                {batch.待处理项.length === 0 ? '—' : batch.待处理项.map((item) => (
                  <div key={item.源编号}>{item.源编号}：待处理，位点 {item.最后确认位点}</div>
                ))}
              </td>
            </tr>
          ))}
          {batches.length === 0 && (
            <tr>
              <td className="empty-state" colSpan={6}>暂无批次，勾选流数据源后执行批量暂停</td>
            </tr>
          )}
        </tbody>
      </table>

      <footer className="page-foot">
        <span>共 {total} 条流数据源接入记录</span>
        {errorMessage && <span className="error-text">{errorMessage}</span>}
      </footer>
    </section>
  )
}
