import { useCallback, useEffect, useMemo, useState } from 'react'

import { request } from '@/api/client'

const ENDPOINT = '/api/metric'
const columns = [
  "采集器编号",
  "作业名称",
  "指标名称",
  "采集间隔",
  "聚合方式",
  "保留天数",
  "最近采集",
  "采集器状态"
]
const actions = [
  "启动采集",
  "暂停采集",
  "关闭采集"
]
const statuses = [
  "草稿",
  "采集中",
  "已暂停",
  "已关闭"
]
const stats = [
  {
    "label": "指标采集器总数",
    "value": 0
  },
  {
    "label": "采集中采集器",
    "value": 0
  },
  {
    "label": "已暂停采集器",
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

type EffectiveOffset = {
  源编号: string
  消费组: string
  分区: number
  生效位点: number
  位点口径: string
  分区状态: string
}

export default function MetricView() {
  const [rows, setRows] = useState<Row[]>([])
  const [total, setTotal] = useState(0)
  const [errorMessage, setErrorMessage] = useState('')
  const [filters, setFilters] = useState<Record<string, string>>({})
  const [batches, setBatches] = useState<ResumeBatch[]>([])
  const [effectiveOffsets, setEffectiveOffsets] = useState<EffectiveOffset[]>([])
  const filterFields = useMemo(() => columns.slice(0, 3), [])

  const reload = useCallback(async () => {
    setErrorMessage('')
    const query = new URLSearchParams(filters).toString()
    try {
      const response = await request(`${ENDPOINT}?${query}`)
      if (!response.ok) {
        throw new Error('指标采集器列表读取失败')
      }
      const payload = await response.json()
      setRows(payload.items ?? [])
      setTotal(payload.total ?? 0)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '流作业指标采集列表读取失败')
    }
  }, [filters])

  const reloadResume = useCallback(async () => {
    try {
      const [batchResponse, offsetResponse] = await Promise.all([
        request(`${ENDPOINT}/resume-dataset`),
        request(`${ENDPOINT}/resume-dataset/effective-offsets`),
      ])
      if (!batchResponse.ok || !offsetResponse.ok) {
        throw new Error('位点续做数据集读取失败')
      }
      const batchPayload = await batchResponse.json()
      const offsetPayload = await offsetResponse.json()
      setBatches(batchPayload.items ?? [])
      setEffectiveOffsets(offsetPayload.items ?? [])
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '位点续做数据集读取失败')
    }
  }, [])

  useEffect(() => {
    void reload()
    void reloadResume()
  }, [reload, reloadResume])

  async function runAction(action: string, row: Row) {
    setErrorMessage('')
    try {
      const response = await request(`${ENDPOINT}/${row.id}/actions`, {
        method: 'POST',
        body: JSON.stringify({ action }),
      })
      if (!response.ok) {
        throw new Error('流作业指标采集动作未生效，请稍后重试')
      }
      await reload()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '流作业指标采集操作失败')
    }
  }

  async function writeback(batchId: number, item: ResumeItem, result: '成功' | '失败') {
    setErrorMessage('')
    try {
      const response = await request(`${ENDPOINT}/resume-dataset/${batchId}/writeback`, {
        method: 'POST',
        body: JSON.stringify({
          source_code: item.源编号,
          result,
          last_offset: result === '成功' ? item.批次检查点位点 : item.最后确认位点,
        }),
      })
      const payload = await response.json()
      if (!response.ok) {
        throw new Error(payload?.detail ?? '消费侧回写未生效')
      }
      await reloadResume()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '消费侧回写失败')
    }
  }

  const actionableItems = useMemo(() => batches.flatMap((batch) =>
    batch.明细
      .filter((item) => item.处理结果 !== '成功')
      .map((item) => ({ batch, item })),
  ), [batches])

  return (
    <section className="page" data-module="metric">
      <header className="page-head">
        <div>
          <h2>流作业指标采集管理</h2>
          <p className="page-desc">维护流作业指标的采集间隔、聚合方式与保留策略，为流作业指标采集提供可审计的数据工程流程。</p>
        </div>
        <div className="page-actions">
          <button className="btn primary" type="button" onClick={() => setErrorMessage('指标采集器登记入口尚未接入审批流')}>
            登记指标采集器
          </button>
          <button className="btn" type="button" onClick={() => window.open(`${ENDPOINT}/export`, '_blank')}>
            导出流作业指标采集清单
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

      <table className="data-table">
        <thead>
          <tr>
            {columns.map((column) => <th key={column}>{column}</th>)}
            <th>可执行动作</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={String(row.id)}>
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
              <td className="empty-state" colSpan={columns.length + 1}>暂无流作业指标采集数据，可先登记指标采集器</td>
            </tr>
          )}
        </tbody>
      </table>

      <h3 style={{ margin: '24px 0 8px' }}>当前生效位点（只认位点续做数据集，同一分区不重复消费）</h3>
      <table className="data-table">
        <thead>
          <tr>
            <th>源编号</th>
            <th>消费组</th>
            <th>分区</th>
            <th>生效位点</th>
            <th>位点口径</th>
            <th>分区状态</th>
          </tr>
        </thead>
        <tbody>
          {effectiveOffsets.map((item) => (
            <tr key={item.源编号}>
              <td>{item.源编号}</td>
              <td>{item.消费组}</td>
              <td>{item.分区}</td>
              <td>{item.生效位点}</td>
              <td>{item.位点口径}</td>
              <td>{item.分区状态}</td>
            </tr>
          ))}
          {effectiveOffsets.length === 0 && (
            <tr><td className="empty-state" colSpan={6}>暂未读取到生效位点</td></tr>
          )}
        </tbody>
      </table>

      <h3 style={{ margin: '24px 0 8px' }}>位点续做数据集（与流数据源接入页面共用同一份）</h3>
      <p className="page-desc">
        消费侧在此回写自身处理结果；已成功确认的分区重复回写会被拒绝，竞态请求只允许一项生效。
      </p>
      <table className="data-table">
        <thead>
          <tr>
            <th>批次数据集ID</th>
            <th>源编号</th>
            <th>分区</th>
            <th>暂停前最后位点</th>
            <th>批次检查点位点</th>
            <th>最后确认位点</th>
            <th>处理结果</th>
            <th>消费侧回写</th>
          </tr>
        </thead>
        <tbody>
          {batches.flatMap((batch) => batch.明细.map((item) => ({ batch, item }))).map(({ batch, item }) => (
            <tr key={`${batch.id}-${item.源编号}`}>
              <td>{batch.批次数据集ID}</td>
              <td>{item.源编号}</td>
              <td>{item.分区}</td>
              <td>{item.暂停前最后位点}</td>
              <td>{item.批次检查点位点}</td>
              <td>{item.最后确认位点}</td>
              <td>{item.处理结果}</td>
              <td className="row-actions">
                {item.处理结果 === '成功' ? (
                  <span className="stat-label">已确认，不可重复回写</span>
                ) : (
                  <>
                    <button className="link" type="button" onClick={() => void writeback(batch.批次数据集ID, item, '成功')}>
                      回写成功
                    </button>
                    <button className="link" type="button" onClick={() => void writeback(batch.批次数据集ID, item, '失败')}>
                      回写失败
                    </button>
                  </>
                )}
              </td>
            </tr>
          ))}
          {actionableItems.length === 0 && batches.length === 0 && (
            <tr><td className="empty-state" colSpan={8}>暂无位点续做批次</td></tr>
          )}
        </tbody>
      </table>

      <footer className="page-foot">
        <span>共 {total} 条流作业指标采集记录</span>
        {errorMessage && <span className="error-text">{errorMessage}</span>}
      </footer>
    </section>
  )
}
