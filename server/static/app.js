const $ = (id) => document.getElementById(id);
const state = { settings: null, summary: null, currentJobId: null, settingsDirty: false };

async function api(path, options = {}) {
  const res = await fetch(path, { headers: { 'Content-Type': 'application/json' }, ...options });
  if (!res.ok) throw new Error(`${res.status}: ${await res.text()}`);
  return res.json();
}

function pct(done, total) { return total ? Math.round((done / total) * 100) : 0; }
function short(id) { return id ? `${id.slice(0, 8)}…${id.slice(-4)}` : '未设置'; }
function pillClass(status) { return status === 'completed' || status === 'success' ? 'pill good' : status === 'failed' ? 'pill bad' : 'pill'; }

function progressBar(done, total) {
  return `<div class="progress"><i style="width:${Math.min(100, pct(done, total))}%"></i></div>`;
}

function renderOverview(summary) {
  const run = summary.active_run;
  $('activeRunPill').textContent = short(summary.active_run_id);
  if (!run) {
    $('overview').innerHTML = `<p class="subtle">还没有 active run。可以创建新 run，或者在设置里指定 active_run_id。</p>`;
    return;
  }
  const total = run.item_count || 0;
  const failed = run.failed || 0;
  $('overview').innerHTML = `
    <div class="metric-row">
      <div class="metric"><span class="subtle">总数</span><strong>${total}</strong></div>
      <div class="metric"><span class="subtle">已抽帧</span><strong>${run.framed || 0}</strong></div>
      <div class="metric"><span class="subtle">已判分</span><strong>${run.classified || 0}</strong></div>
      <div class="metric"><span class="subtle">失败</span><strong>${failed}</strong></div>
    </div>
    ${stageCard('frames', run.stages?.frames, total)}
    ${stageCard('classify', run.stages?.classify, total)}
    ${stageCard('export', run.stages?.export, total)}
    ${stageCard('organize', run.stages?.organize, total)}
    <p class="subtle">CSV：${run.csv_path ? `<code>${run.csv_path}</code>` : '尚未导出'}</p>`;
}

function stageCard(name, data = {}, total) {
  const done = (data.success || 0) + (data.skipped || 0);
  return `<div class="stage">
    <div class="stage-head"><strong>${name}</strong><span class="subtle">${done}/${total} · F ${data.failed || 0}</span></div>
    ${progressBar(done, total)}
  </div>`;
}

function renderSettings(payload) {
  state.settings = payload.settings;
  const a = payload.settings.analysis || {};
  const p = a.provider || {};
  const fields = [
    ['path','下载目录',payload.settings.path], ['thread','线程',payload.settings.thread],
    ['rate_limit','下载限速',payload.settings.rate_limit], ['proxy','代理',payload.settings.proxy || ''],
    ['retry_times','下载重试',payload.settings.retry_times], ['analysis.active_run_id','active_run_id',a.active_run_id || ''],
    ['analysis.organize_run_id','organize_run_id',a.organize_run_id || ''], ['analysis.batch_size','判分批量',a.batch_size],
    ['analysis.frame_count','抽帧数',a.frame_count], ['analysis.grid_rows','网格行',a.grid_rows],
    ['analysis.grid_cols','网格列',a.grid_cols], ['analysis.provider.model','模型',p.model || ''],
    ['analysis.provider.base_url','Base URL',p.base_url || ''], ['analysis.provider.timeout','超时',p.timeout],
    ['analysis.provider.rate_limit','模型限速',p.rate_limit], ['analysis.provider.retry_times','模型重试',p.retry_times],
  ];
  $('settingsForm').innerHTML = fields.map(([key,label,value]) => `
    <label title="${payload.fields[key] || ''}">${label}<input data-key="${key}" value="${value ?? ''}" /></label>
  `).join('') + `
    <label>尾批继续<select data-key="analysis.allow_partial_batch"><option value="true" ${a.allow_partial_batch ? 'selected' : ''}>true</option><option value="false" ${!a.allow_partial_batch ? 'selected' : ''}>false</option></select></label>
    <label>调试停机<select data-key="analysis.provider.debug_stop_on_api_error"><option value="true" ${p.debug_stop_on_api_error ? 'selected' : ''}>true</option><option value="false" ${!p.debug_stop_on_api_error ? 'selected' : ''}>false</option></select></label>
    <label class="setting-wide">buckets JSON<textarea data-key="analysis.buckets">${JSON.stringify(a.buckets || [], null, 2)}</textarea></label>
    <p class="subtle setting-wide">敏感字段：API Key ${payload.secrets['analysis.provider.api_key'] ? '已设置' : '未设置'}，Cookie ${payload.secrets.cookie ? '已设置' : '未设置'}。不会在页面回显。</p>`;
  document.querySelectorAll('[data-key]').forEach(el => {
    el.addEventListener('input', () => { state.settingsDirty = true; });
  });
}

function collectSettingsPatch() {
  const patch = { analysis: { provider: {} } };
  document.querySelectorAll('[data-key]').forEach(el => {
    const key = el.dataset.key;
    let value = el.value;
    if (value === 'true') value = true;
    else if (value === 'false') value = false;
    else if (['thread','retry_times','analysis.batch_size','analysis.frame_count','analysis.grid_rows','analysis.grid_cols','analysis.provider.timeout','analysis.provider.rate_limit','analysis.provider.retry_times'].includes(key)) value = Number(value || 0);
    else if (key === 'analysis.buckets') value = JSON.parse(value || '[]');
    if (key.startsWith('analysis.provider.')) patch.analysis.provider[key.replace('analysis.provider.','')] = value;
    else if (key.startsWith('analysis.')) patch.analysis[key.replace('analysis.','')] = value;
    else patch[key] = value;
  });
  return patch;
}

function renderJob(job) {
  if (!job) return;
  $('jobStatus').textContent = job.status;
  $('jobStatus').className = pillClass(job.status);
  const stages = Object.entries(job.stages || {}).map(([name, s]) => `
    <div class="stage"><div class="stage-head"><strong>${s.label || name}</strong><span>${s.completed || 0}/${s.total || 0}</span></div>${progressBar(s.completed || 0, s.total || 0)}<p class="subtle">${s.detail || ''}</p></div>
  `).join('');
  $('jobDetail').innerHTML = `
    <p class="subtle">job <code>${job.job_id}</code> · run <code>${job.run_id || '等待创建'}</code></p>
    ${stages || '<p class="subtle">等待后台任务开始…</p>'}
    ${job.error ? `<p class="pill bad">${job.error}</p>` : ''}
    ${job.debug_report ? `<textarea readonly>${job.debug_report}</textarea>` : ''}`;
}

function renderRuns(runs) {
  $('runs').innerHTML = runs.map(r => {
    const total = r.item_count || 0;
    return `<div class="run-item">
      <div class="run-head"><strong><code>${r.run_id}</code></strong><span class="${pillClass(r.status)}">${r.status}</span></div>
      <div class="metric-row">
        <div class="metric"><span class="subtle">抽帧</span><strong>${r.framed}/${total}</strong></div>
        <div class="metric"><span class="subtle">判分</span><strong>${r.classified}/${total}</strong></div>
        <div class="metric"><span class="subtle">归类</span><strong>${r.organized}/${total}</strong></div>
        <div class="metric"><span class="subtle">失败</span><strong>${r.failed}</strong></div>
      </div>
    </div>`;
  }).join('') || '<p class="subtle">暂无 run。</p>';
}

function renderScores(data) {
  if (!data) { $('scores').innerHTML = '<p class="subtle">暂无评分数据。</p>'; return; }
  $('scoreRunPill').textContent = short(data.run_id);
  const dist = data.distributions?.[data.primary_attribute] || {};
  const max = Math.max(1, ...Object.values(dist));
  const bars = Object.entries(dist).map(([score,count]) => `
    <div class="bar-line"><span>${score} 分</span>${progressBar(count, max)}<strong>${count}</strong></div>
  `).join('');
  const buckets = (data.buckets || []).map(b => `<span class="pill">${b.label}: ${b.count}</span>`).join(' ');
  $('scores').innerHTML = `<p class="subtle">主字段：${data.primary_attribute} · 已评分 ${data.scored_items}</p><div class="bars">${bars || '无分布'}</div><div>${buckets}</div>`;
}

async function loadAll() {
  try {
    $('serviceStatus').textContent = '在线'; $('serviceStatus').className = 'pill good';
    const [settings, summary, runs] = await Promise.all([
      api('/api/v1/settings'), api('/api/v1/pipeline/summary'), api('/api/v1/pipeline/runs?limit=20')
    ]);
    state.summary = summary;
    if (!state.settingsDirty) renderSettings(settings); renderOverview(summary); renderRuns(runs.runs || []);
    const scoreRun = summary.active_run?.run_id || summary.latest_classified_run?.run_id;
    if (scoreRun) renderScores(await api(`/api/v1/pipeline/runs/${scoreRun}/scores`));
    const activeJob = (summary.jobs || []).reverse().find(j => !['success','failed'].includes(j.status));
    if (activeJob) { state.currentJobId = activeJob.job_id; renderJob(activeJob); }
  } catch (err) {
    $('serviceStatus').textContent = '异常'; $('serviceStatus').className = 'pill bad';
    console.error(err);
  }
}

async function pollJob() {
  if (!state.currentJobId) return;
  try {
    const job = await api(`/api/v1/pipeline/jobs/${state.currentJobId}`);
    renderJob(job);
    if (['success','failed'].includes(job.status)) { state.currentJobId = null; await loadAll(); }
  } catch (err) { console.error(err); }
}

$('refreshBtn').onclick = loadAll;
$('saveSettingsBtn').onclick = async () => {
  try { await api('/api/v1/settings', { method: 'PATCH', body: JSON.stringify({ settings: collectSettingsPatch() }) }); $('settingsMessage').textContent = '已保存'; await loadAll(); }
  catch (err) { $('settingsMessage').textContent = `保存失败：${err.message}`; }
};
$('startJobBtn').onclick = async () => {
  const body = { action: $('actionSelect').value, run_id: $('runIdInput').value || null, limit: Number($('limitInput').value || 0) };
  try { const job = await api('/api/v1/pipeline/jobs', { method: 'POST', body: JSON.stringify(body) }); state.currentJobId = job.job_id; renderJob(job); }
  catch (err) { $('jobDetail').innerHTML = `<p class="pill bad">启动失败：${err.message}</p>`; }
};

loadAll();
setInterval(pollJob, 1200);
setInterval(loadAll, 8000);
