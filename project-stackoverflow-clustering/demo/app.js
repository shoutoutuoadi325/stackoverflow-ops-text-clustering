const data = window.DEMO_DATA || {};

const fmt = new Intl.NumberFormat("zh-CN");

function number(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function text(value) {
  return String(value ?? "");
}

function byId(id) {
  return document.getElementById(id);
}

function renderMetrics() {
  const metrics = data.metrics || {};
  const topicMetric = (data.topicMetric || [])[0] || {};
  const validation = data.rawValidation?.metrics || {};
  const cards = [
    ["问题数", validation.questions ?? metrics.questions],
    ["回答数", validation.answers ?? metrics.answers],
    ["无回答问题", validation.unanswered_questions ?? metrics.unanswered_questions],
    ["主题聚类 Silhouette", Number(topicMetric.silhouette || 0).toFixed(4)],
  ];

  byId("metrics").innerHTML = cards
    .map(([label, value]) => `<div class="metric"><span>${label}</span><strong>${typeof value === "number" ? fmt.format(value) : value}</strong></div>`)
    .join("");
}

function renderValidation() {
  const validation = data.rawValidation || {};
  const metrics = validation.metrics || {};
  const rows = [
    ["源文件", validation.source_path],
    ["文件大小", validation.file_size_bytes ? `${fmt.format(number(validation.file_size_bytes))} bytes` : "未生成"],
    ["SHA256", validation.sha256 || "未生成"],
    ["核验时间", validation.validated_at_utc || "未生成"],
    ["问题数", metrics.questions],
    ["回答数", metrics.answers],
    ["问题评论", metrics.question_comments],
    ["回答评论", metrics.answer_comments],
    ["最高分问题", metrics.max_question_id ? `${metrics.max_question_id} / ${metrics.max_question_score}` : "未生成"],
  ];
  byId("validationGrid").innerHTML = rows
    .map(([label, value]) => `<div class="validation-item"><span>${label}</span><strong>${value ?? "未生成"}</strong></div>`)
    .join("");

  const sources = data.dataSources || {};
  byId("sourceList").innerHTML = Object.entries(sources)
    .map(([label, value]) => `<div class="source-item"><span>${label}</span><strong>${value || "未生成"}</strong></div>`)
    .join("");
}

function renderEvaluation() {
  const rec = data.recommendation || {};
  byId("recommendation").innerHTML = `<strong>${rec.run_name || "暂无推荐参数"}</strong><p>${rec.reason || "未生成参数扫描结果。"}</p>`;

  byId("sweepRows").innerHTML =
    (data.parameterSweep || [])
      .map(
        (row) => `<tr>
          <td>${text(row.run_name)}</td>
          <td>${number(row.similarity_threshold).toFixed(2)}</td>
          <td>${text(row.num_hash_tables)}</td>
          <td>${fmt.format(number(row.candidate_pair_count))}</td>
          <td>${fmt.format(number(row.pair_count))}</td>
          <td>${fmt.format(number(row.multi_doc_cluster_count))}</td>
          <td>${fmt.format(number(row.max_cluster_size))}</td>
          <td>${row.elapsed_seconds ? `${fmt.format(number(row.elapsed_seconds))}s` : "未生成"}</td>
        </tr>`,
      )
      .join("") || `<tr><td colspan="8">未生成参数扫描结果。运行 05_submit_cluster_sweep.sh 并导出后刷新 Demo。</td></tr>`;

  const review = data.reviewMetrics || {};
  const precision = review.precision === null || review.precision === undefined ? "待人工审核" : number(review.precision).toFixed(4);
  byId("reviewSummary").textContent = `状态：${review.status || "未生成"}，已标注：${review.labeled_count || 0}，precision：${precision}`;
  byId("reviewRows").innerHTML =
    (data.reviewCandidates || [])
      .slice(0, 80)
      .map(
        (row) => `<tr>
          <td>[${text(row.src)}] ${text(row.src_title)}<br />[${text(row.dst)}] ${text(row.dst_title)}</td>
          <td>${number(row.similarity).toFixed(3)}</td>
          <td>${text(row.threshold)}</td>
          <td>${text(row.label) || "待审核"}</td>
        </tr>`,
      )
      .join("") || `<tr><td colspan="4">未生成 review_candidates.csv。</td></tr>`;
}

function renderBars(containerId, rows, labelKey = "tag") {
  const max = Math.max(...rows.map((row) => number(row.count)), 1);
  byId(containerId).innerHTML = rows
    .slice(0, 10)
    .map((row) => {
      const value = number(row.count);
      const width = Math.max(4, (value / max) * 100);
      return `<div class="bar-row">
        <span title="${text(row[labelKey])}">${text(row[labelKey])}</span>
        <span class="bar-track"><span class="bar-fill" style="width:${width}%"></span></span>
        <span class="bar-value">${fmt.format(value)}</span>
      </div>`;
    })
    .join("");
}

function renderTopicRows() {
  byId("topicRows").innerHTML = (data.topicSamples || [])
    .slice(0, 14)
    .map(
      (row) => `<tr>
        <td>${row.cluster_id}</td>
        <td>${text(row.title)}</td>
        <td>${fmt.format(number(row.score))}</td>
        <td>${text(row.cluster_top_terms)}</td>
      </tr>`,
    )
    .join("");
}

function clusterMatches(row, query) {
  const haystack = [row.doc_id, row.cluster_id, row.title, row.tags_text, row.representative_title].map(text).join(" ").toLowerCase();
  return haystack.includes(query);
}

function renderClusters() {
  const query = byId("clusterFilter").value.trim().toLowerCase();
  const sort = byId("clusterSort").value;
  const rows = (data.duplicateClusters || []).filter((row) => clusterMatches(row, query));

  rows.sort((a, b) => {
    if (sort === "size") return number(b.cluster_size) - number(a.cluster_size);
    if (sort === "score") return number(b.score) - number(a.score);
    return number(b.avg_similarity) - number(a.avg_similarity);
  });

  byId("clusterList").innerHTML =
    rows
      .slice(0, 40)
      .map(
        (row) => `<article class="cluster-item">
          <div><strong>[${text(row.doc_id)}]</strong> ${text(row.title)}</div>
          <div class="cluster-meta">
            <span class="pill">cluster ${text(row.cluster_id)}</span>
            <span>规模 ${fmt.format(number(row.cluster_size))}</span>
            <span>平均相似度 ${number(row.avg_similarity).toFixed(3)}</span>
            <span>代表问题 ${text(row.representative_id)}</span>
          </div>
        </article>`,
      )
      .join("") || `<div class="result-summary">没有匹配的重复簇。可以先尝试 analytic function 或 group by。</div>`;
}

function renderPairs() {
  byId("pairList").innerHTML =
    (data.similarPairs || [])
      .slice()
      .sort((a, b) => number(b.similarity) - number(a.similarity))
      .map(
        (row) => `<article class="pair-item">
          <div><strong>[${text(row.src)}]</strong> ${text(row.src_title)}</div>
          <div><strong>[${text(row.dst)}]</strong> ${text(row.dst_title)}</div>
          <div class="pair-meta"><span class="pill">similarity ${number(row.similarity).toFixed(3)}</span><span>${text(row.source || "baseline")}</span><span>src score ${number(row.src_score)}</span><span>dst score ${number(row.dst_score)}</span></div>
        </article>`,
      )
      .join("") || `<div class="result-summary">当前样例没有相似问题对，请运行更高召回参数后重新生成 demo/data.js。</div>`;
}

function searchRows(query) {
  const q = query.trim().toLowerCase();
  if (!q) return [];
  const exactId = /^\d+$/.test(q);
  const results = [];

  for (const row of data.duplicateClusters || []) {
    if ((exactId && text(row.doc_id) === q) || clusterMatches(row, q)) {
      results.push(["重复簇", text(row.doc_id || row.cluster_id), text(row.title), `score ${number(row.score)} / sim ${number(row.avg_similarity).toFixed(3)}`]);
    }
  }

  for (const row of data.similarPairs || []) {
    const haystack = [row.src, row.dst, row.src_title, row.dst_title].map(text).join(" ").toLowerCase();
    if (haystack.includes(q)) {
      results.push(["相似对", `${text(row.src)} - ${text(row.dst)}`, `${text(row.src_title)} / ${text(row.dst_title)}`, `sim ${number(row.similarity).toFixed(3)}`]);
    }
  }

  for (const row of data.topicSamples || []) {
    const haystack = [row.doc_id, row.title, row.tags_text, row.cluster_top_terms].map(text).join(" ").toLowerCase();
    if (haystack.includes(q)) {
      results.push(["主题样例", `cluster ${text(row.cluster_id)}`, text(row.title), `score ${number(row.score)}`]);
    }
  }

  return results.slice(0, 80);
}

function runSearch() {
  const query = byId("queryInput").value;
  const rows = searchRows(query);
  byId("resultSummary").textContent = `找到 ${rows.length} 条展示结果`;
  byId("searchRows").innerHTML =
    rows
      .map(
        ([source, id, title, metric]) => `<tr>
          <td>${source}</td>
          <td>${id}</td>
          <td>${title}</td>
          <td>${metric}</td>
        </tr>`,
      )
      .join("") || `<tr><td colspan="4">没有命中。建议尝试 analytic function、group by、select、ora-00904。</td></tr>`;
}

function bindEvents() {
  document.querySelectorAll(".tab-button").forEach((button) => {
    button.addEventListener("click", () => {
      document.querySelectorAll(".tab-button").forEach((item) => item.classList.remove("active"));
      document.querySelectorAll(".view").forEach((item) => item.classList.remove("active"));
      button.classList.add("active");
      byId(button.dataset.view).classList.add("active");
    });
  });
  byId("clusterFilter").addEventListener("input", renderClusters);
  byId("clusterSort").addEventListener("change", renderClusters);
  byId("queryButton").addEventListener("click", runSearch);
  byId("queryInput").addEventListener("keydown", (event) => {
    if (event.key === "Enter") runSearch();
  });
}

function renderTopicDedup() {
  const rows = (data.topicDedupView || [])
    .slice()
    .sort((a, b) => number(b.dedup_ratio) - number(a.dedup_ratio));
  const body = byId("topicDedupRows");
  if (!body) return;
  body.innerHTML =
    rows
      .slice(0, 30)
      .map(
        (row) => `<tr>
          <td>${number(row.topic_id)}</td>
          <td>${fmt.format(number(row.topic_size))}</td>
          <td>${fmt.format(number(row.duplicate_cluster_count))}</td>
          <td>${fmt.format(number(row.duplicate_question_count))}</td>
          <td>${(number(row.dedup_ratio) * 100).toFixed(2)}%</td>
          <td>${text(row.top_terms)}</td>
        </tr>`,
      )
      .join("") || `<tr><td colspan="6">未生成 topic_dedup_view，请先运行 13_submit_topic_dedup_view.sh 并导出。</td></tr>`;
}

function renderOra() {
  const summary = data.oraSummary || {};
  const summaryEl = byId("oraSummary");
  if (summaryEl) {
    const total = number(summary.total_questions);
    const withOra = number(summary.questions_with_ora_code);
    const ratio = total ? ((withOra / total) * 100).toFixed(2) : "0.00";
    const distinct = number(summary.distinct_ora_codes);
    const occurrences = number(summary.total_ora_occurrences);
    summaryEl.innerHTML = `
      <div class="metric"><span>含 ORA 码问题</span><strong>${fmt.format(withOra)}</strong></div>
      <div class="metric"><span>覆盖率</span><strong>${ratio}%</strong></div>
      <div class="metric"><span>独立 ORA 码</span><strong>${fmt.format(distinct)}</strong></div>
      <div class="metric"><span>总出现次数</span><strong>${fmt.format(occurrences)}</strong></div>
    `;
  }

  const renderList = (containerId, rows, fallback) => {
    const el = byId(containerId);
    if (!el) return;
    el.innerHTML =
      (rows || [])
        .slice(0, 30)
        .map(
          (row) => `<article class="pair-item">
            <div><strong>[${text(row.src)}]</strong> ${text(row.src_title)}</div>
            <div><strong>[${text(row.dst)}]</strong> ${text(row.dst_title)}</div>
            <div class="pair-meta">
              <span class="pill">similarity ${number(row.similarity).toFixed(3)}</span>
              <span>raw ${number(row.similarity_raw).toFixed(3)}</span>
              <span>ORA ${text(row.shared_ora_codes) || "-"}</span>
              <span>src ${number(row.src_score)} / dst ${number(row.dst_score)}</span>
            </div>
          </article>`,
        )
        .join("") || `<div class="result-summary">${fallback}</div>`;
  };
  renderList("oraRescuedList", data.oraRescuedPairs, "未生成 ORA 救回样例。运行 11_submit_ora_stats.sh 后再生成 demo/data.js。");
  renderList("oraMatchedList", data.oraMatchedPairs, "未生成共享 ORA 的相似对样例。");
}

function init() {
  renderMetrics();
  renderValidation();
  renderEvaluation();
  renderBars("tagBars", data.topTags || [], "tag");
  renderBars("oraBars", data.topOraCodes || [], "token");
  renderTopicRows();
  renderTopicDedup();
  renderOra();
  renderClusters();
  renderPairs();
  runSearch();
  bindEvents();
}

init();
