const { createApp, onMounted, onBeforeUnmount, ref, computed, watch } = Vue;

createApp({
  setup() {
    const apiBase = window.location.origin.replace(/\/+$/, '');
    const pages = [
      { key: 'overview', label: 'Overview' },
      { key: 'workers', label: 'Workers' },
      { key: 'mirrors', label: 'Mirrors' },
    ];
    const activePage = ref('overview');
    const overview = ref({
      workers_total: 0,
      workers_approved: 0,
      workers_pending: 0,
      workers_rejected: 0,
      resources_total: 0,
    });
    const workers = ref([]);
    const resources = ref([]);
    const loading = ref({ overview: true, workers: true, resources: true, telemetry: true });
    const banner = ref('');
    const modalNotice = ref({ message: '', kind: 'info' });
    const lastUpdated = ref(null);
    const isBusy = computed(() => Object.values(loading.value).some(Boolean));
    const activeWorker = ref(null);
    const syncPageFromHash = () => {
      const hash = window.location.hash.replace('#', '').toLowerCase();
      if (pages.some((p) => p.key === hash)) {
        activePage.value = hash;
      }
    };
    const setPage = (pageKey) => {
      if (pages.some((p) => p.key === pageKey)) {
        activePage.value = pageKey;
      }
    };

    const generateTrafficSeries = () =>
      Array.from({ length: 24 }, (_, idx) => {
        const minutesAgo = 23 - idx;
        const angle = (idx / 23) * Math.PI;
        const base = 420 + Math.sin(angle) * 90 + Math.random() * 40;
        return { label: `${minutesAgo}m`, value: Math.max(120, Math.round(base)) };
      });

    const defaultTelemetry = () => ({
      traffic: generateTrafficSeries(),
      sync: { success: 1280, failure: 42 },
      disk: { total_gb: 1024, used_gb: 612 },
      message_rate: 3200,
      latency_ms: { p50: 45, p95: 110, p99: 180 },
      concurrent_jobs: 58,
      last_sample_at: Math.floor(Date.now() / 1000),
    });

    const telemetry = ref(defaultTelemetry());
    const newReplica = ref({
      name: '',
      kind: 'git',
      upstream: '',
      interval_secs: 300,
      labels_text: '',
    });

    const numberFormatter = new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 });
    const formatNumber = (value) => numberFormatter.format(Math.max(0, Math.round(value || 0)));

    const fetchJson = async (path, opts) => {
      const res = await fetch(path, opts);
      if (!res.ok) throw new Error(await res.text());
      return res.json();
    };

    const normalizeTelemetry = (data = {}) => {
      const fallback = defaultTelemetry();
      const incoming = Array.isArray(data.traffic) && data.traffic.length
        ? data.traffic.map((point, idx) => ({
          label: point && point.label ? point.label : `${idx}m`,
          value: Number(point && point.value) || 0,
        }))
        : fallback.traffic;

      return {
        traffic: incoming,
        sync: {
          success: Number(data.sync?.success ?? fallback.sync.success),
          failure: Number(data.sync?.failure ?? fallback.sync.failure),
        },
        disk: {
          total_gb: Number(data.disk?.total_gb ?? fallback.disk.total_gb),
          used_gb: Number(data.disk?.used_gb ?? fallback.disk.used_gb),
        },
        message_rate: Number(data.message_rate ?? fallback.message_rate),
        latency_ms: data.latency_ms || fallback.latency_ms,
        concurrent_jobs: Number(data.concurrent_jobs ?? fallback.concurrent_jobs),
        last_sample_at: data.last_sample_at ?? fallback.last_sample_at,
      };
    };

    const buildSparklinePaths = (values, width = 420, height = 140) => {
      if (!values.length) {
        const baseline = height - 4;
        return {
          line: `M0 ${baseline} L${width} ${baseline}`,
          area: `M0 ${height} L${width} ${height} L0 ${height} Z`,
        };
      }
      const max = Math.max(...values);
      const min = Math.min(...values);
      const range = max - min || 1;
      const step = values.length > 1 ? width / (values.length - 1) : width;
      let path = '';
      values.forEach((val, idx) => {
        const x = idx * step;
        const normalized = (val - min) / range;
        const y = height - normalized * (height - 12) - 6;
        path += `${idx === 0 ? 'M' : 'L'}${x.toFixed(1)} ${y.toFixed(1)} `;
      });
      const trimmed = path.trim();
      const area = `${trimmed} L${width.toFixed(1)} ${height} L0 ${height} Z`;
      return { line: trimmed, area };
    };

    const loadOverview = async () => {
      loading.value.overview = true;
      try {
        const data = await fetchJson(`${apiBase}/overview`);
        overview.value = { workers_rejected: 0, ...data };
      } finally {
        loading.value.overview = false;
      }
    };

    const loadWorkers = async () => {
      loading.value.workers = true;
      try {
        workers.value = await fetchJson(`${apiBase}/workers`);
      } finally {
        loading.value.workers = false;
      }
    };

    const loadResources = async () => {
      loading.value.resources = true;
      try {
        resources.value = await fetchJson(`${apiBase}/resources`);
      } finally {
        loading.value.resources = false;
      }
    };

    const loadTelemetry = async () => {
      loading.value.telemetry = true;
      try {
        const data = await fetchJson(`${apiBase}/telemetry`);
        telemetry.value = normalizeTelemetry(data);
      } catch (err) {
        console.warn('telemetry fetch failed', err);
        telemetry.value = defaultTelemetry();
      } finally {
        loading.value.telemetry = false;
      }
    };

    const send = async (path, opts) => {
      const res = await fetch(path, opts);
      if (!res.ok) throw new Error(await res.text());
    };

    const setNotice = (msg, kind = 'info') => {
      if (activeWorker.value) {
        modalNotice.value = { message: msg, kind };
      } else {
        banner.value = msg;
      }
    };

    const handleAction = async (label, fn, onSuccess) => {
      try {
        await fn();
        await refreshAll();
        if (onSuccess) setNotice(onSuccess);
      } catch (err) {
        console.error(err);
        setNotice(`${label}: ${err.message || 'unexpected error'}`, 'danger');
      }
    };

    const approve = async (id) =>
      handleAction('Approve failed', () =>
        send(`${apiBase}/workers/${id}/approve`, { method: 'POST' }),
      );

    const reject = async (id) =>
      handleAction('Reject failed', () =>
        send(`${apiBase}/workers/${id}/reject`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ reason: 'Rejected from console' }),
        }),
      );

    const terminate = async (id) =>
      handleAction('Terminate failed', () =>
        send(`${apiBase}/workers/${id}`, { method: 'DELETE' }),
      );

    const removeReplica = async (workerId, replicaId) =>
      handleAction(
        'Remove replica failed',
        () =>
          send(`${apiBase}/workers/${workerId}/remove_replica`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ replica_id: replicaId }),
          }),
        'Replica removal triggered',
      );

    const syncReplica = async (workerId, resourceName) =>
      handleAction(
        'Sync failed',
        () =>
          send(`${apiBase}/workers/${workerId}/sync`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ resource: resourceName }),
          }),
        'Sync triggered',
      );

    const refreshAll = async () => {
      banner.value = '';
      modalNotice.value = { message: '', kind: 'info' };
      const results = await Promise.allSettled([
        loadOverview(),
        loadWorkers(),
        loadResources(),
        loadTelemetry(),
      ]);
      const failed = results.find((r) => r.status === 'rejected');
      if (failed) {
        setNotice(failed.reason?.message || 'Failed to reach supervisor API', 'danger');
        return;
      }
      lastUpdated.value = Math.floor(Date.now() / 1000);
      if (activeWorker.value) {
        const updated = workers.value.find((w) => w.worker_id === activeWorker.value.worker_id);
        activeWorker.value = updated || null;
      }
    };

    const statusClass = (status) => {
      switch ((status || '').toLowerCase()) {
        case 'ready':
        case 'success':
          return 'good';
        case 'syncing':
        case 'init':
        case 'pending':
          return 'warn';
        case 'approved':
          return 'good';
        case 'rejected':
          return 'bad';
        default:
          return 'bad';
      }
    };

    const workerStatus = (worker) => {
      const raw = worker?.status || (worker?.approved ? 'approved' : 'pending');
      return (raw || '').toLowerCase();
    };

    const openWorker = (worker) => {
      activeWorker.value = worker;
      newReplica.value = {
        name: '',
        kind: 'git',
        upstream: '',
        interval_secs: 300,
        labels_text: '',
      };
    };

    const closeWorker = () => {
      activeWorker.value = null;
    };

    const parseLabels = (text) => {
      if (!text.trim()) return undefined;
      return text.split(',').reduce((acc, entry) => {
        const [k, v] = entry.split('=').map((s) => s?.trim()).filter(Boolean);
        if (k && v !== undefined) acc[k] = v;
        return acc;
      }, {});
    };

    const addReplica = async () => {
      if (!activeWorker.value) return;
      const payload = {
        name: newReplica.value.name.trim(),
        kind: newReplica.value.kind,
        upstream: newReplica.value.upstream.trim(),
        interval_secs: Number(newReplica.value.interval_secs) || null,
        labels: parseLabels(newReplica.value.labels_text),
      };
      if (!payload.name || !payload.upstream) {
        banner.value = 'Name and upstream are required';
        return;
      }
      await handleAction(
        'Add replica failed',
        () =>
          send(`${apiBase}/workers/${activeWorker.value.worker_id}/add_replica`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
          }),
        'Replica add command sent',
      );
    };

    const formatAbsolute = (tsSeconds) => {
      if (!tsSeconds) return '-';
      return new Date(tsSeconds * 1000).toLocaleString();
    };

    const formatRelative = (tsSeconds) => {
      if (!tsSeconds) return '-';
      const diff = Math.max(0, Math.floor(Date.now() / 1000) - tsSeconds);
      if (diff < 60) return `${diff}s ago`;
      if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
      if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
      return `${Math.floor(diff / 86400)}d ago`;
    };

    const trafficSeries = computed(() => (telemetry.value && telemetry.value.traffic ? telemetry.value.traffic : []));
    const trafficValues = computed(() => trafficSeries.value.map((point) => Number(point.value) || 0));
    const trafficCurrent = computed(() => {
      const vals = trafficValues.value;
      return vals.length ? vals[vals.length - 1] : 0;
    });
    const trafficAverage = computed(() => {
      const vals = trafficValues.value;
      if (!vals.length) return 0;
      return vals.reduce((acc, val) => acc + val, 0) / vals.length;
    });
    const trafficTrend = computed(() => {
      const vals = trafficValues.value;
      if (vals.length < 2) return 0;
      const first = vals[0] || 1;
      return ((trafficCurrent.value - first) / (first || 1)) * 100;
    });
    const trafficPaths = computed(() => buildSparklinePaths(trafficValues.value));
    const trafficLinePath = computed(() => trafficPaths.value.line);
    const trafficAreaPath = computed(() => trafficPaths.value.area);

    const syncTotals = computed(() => {
      const success = telemetry.value?.sync?.success ?? 0;
      const failure = telemetry.value?.sync?.failure ?? 0;
      const total = success + failure;
      const rate = total ? (success / total) * 100 : 0;
      const failureRate = total ? (failure / total) * 100 : 0;
      return { success, failure, total, rate, failureRate };
    });

    const diskStats = computed(() => {
      const total = telemetry.value?.disk?.total_gb ?? 0;
      const used = Math.min(total, telemetry.value?.disk?.used_gb ?? 0);
      const remaining = Math.max(total - used, 0);
      const usedPercent = total ? (used / total) * 100 : 0;
      const remainingPercent = 100 - usedPercent;
      return { total, used, remaining, usedPercent, remainingPercent };
    });

    const diskRingStyle = computed(() => {
      const degrees = (diskStats.value.remainingPercent / 100) * 360;
      return {
        background: `conic-gradient(var(--accent-2) 0deg ${degrees}deg, rgba(255, 255, 255, 0.08) ${degrees}deg 360deg)`,
      };
    });

    watch(activePage, (page) => {
      if (window.location.hash !== `#${page}`) {
        window.location.hash = page;
      }
      if (page !== 'workers') {
        activeWorker.value = null;
      }
    });

    onMounted(() => {
      syncPageFromHash();
      window.addEventListener('hashchange', syncPageFromHash);
      refreshAll();
    });

    onBeforeUnmount(() => {
      window.removeEventListener('hashchange', syncPageFromHash);
    });

    return {
      pages,
      activePage,
      setPage,
      apiBase,
      overview,
      workers,
      resources,
      telemetry,
      loading,
      banner,
      lastUpdated,
      isBusy,
      modalNotice,
      activeWorker,
      newReplica,
      approve,
      reject,
      terminate,
      removeReplica,
      syncReplica,
      addReplica,
      refreshAll,
      loadTelemetry,
      statusClass,
      workerStatus,
      openWorker,
      closeWorker,
      formatAbsolute,
      formatRelative,
      formatNumber,
      trafficSeries,
      trafficCurrent,
      trafficAverage,
      trafficTrend,
      trafficLinePath,
      trafficAreaPath,
      syncTotals,
      diskStats,
      diskRingStyle,
    };
  },
}).mount('#app');
