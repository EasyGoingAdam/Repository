/**
 * First Strike — System Health Monitor
 * Polls /api/health every 60s and surfaces errors as a sticky bottom banner.
 * Included in every page via <script src="/static/health.js"></script>
 */
(function () {
  'use strict';

  // ── Create banner element ──────────────────────────────────────────────────
  const banner = document.createElement('div');
  banner.id = 'fs-health-banner';
  banner.style.cssText = [
    'position:fixed', 'bottom:0', 'left:0', 'right:0', 'z-index:99999',
    'background:#0d0505', 'border-top:2px solid #ef4444',
    'padding:8px 48px 8px 16px',
    'display:none', 'flex-wrap:wrap', 'gap:6px', 'align-items:center',
    "font-family:'Courier New',monospace", 'font-size:11px',
    'color:#ef4444', 'min-height:36px',
  ].join(';');

  // Dismiss button
  const closeBtn = document.createElement('button');
  closeBtn.innerHTML = '✕';
  closeBtn.title = 'Dismiss (re-checks in 60s)';
  closeBtn.style.cssText = [
    'position:absolute', 'right:12px', 'top:50%', 'transform:translateY(-50%)',
    'background:none', 'border:none', 'color:#ef4444', 'cursor:pointer',
    'font-size:14px', 'line-height:1', 'padding:4px',
  ].join(';');
  closeBtn.onclick = () => { banner.style.display = 'none'; _dismissed = true; };
  banner.appendChild(closeBtn);

  document.addEventListener('DOMContentLoaded', () => {
    document.body.appendChild(banner);
  });
  // Also append immediately if DOM is already ready
  if (document.readyState !== 'loading') {
    document.body.appendChild(banner);
  }

  let _dismissed = false;

  // ── HTML escape helper ─────────────────────────────────────────────────────
  function esc(s) {
    return String(s || '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;');
  }

  // ── Severity-aware label map ───────────────────────────────────────────────
  // Critical checks cause a red banner; config-only issues use yellow.
  const CRITICAL = new Set(['database', 'polymarket_api', 'intel_loop']);
  const WARN_ONLY = new Set(['twitter_api', 'email_config', 'youtube_api', 'signals']);

  // Friendly labels
  const LABELS = {
    database:       'DB',
    polymarket_api: 'Polymarket API',
    intel_loop:     'Intel Loop',
    twitter_api:    'Twitter API',
    email_config:   'Email',
    youtube_api:    'YouTube API',
    signals:        'Signals',
  };

  // ── Render banner ──────────────────────────────────────────────────────────
  function render(errors) {
    if (!errors || errors.length === 0) {
      banner.style.display = 'none';
      _dismissed = false;
      return;
    }

    if (_dismissed) return;  // user closed it; will re-open on next poll

    const hasCritical = errors.some(e => CRITICAL.has(e.check));
    const color = hasCritical ? '#ef4444' : '#eab308';

    banner.style.borderTopColor = color;
    banner.style.color          = color;

    const chips = errors.map(e => {
      const label = LABELS[e.check] || esc(e.check);
      const msg   = esc(e.error || 'error');
      const bg    = CRITICAL.has(e.check)
        ? 'rgba(239,68,68,.15)'
        : 'rgba(234,179,8,.12)';
      const border = CRITICAL.has(e.check)
        ? 'rgba(239,68,68,.4)'
        : 'rgba(234,179,8,.4)';
      return `<span style="background:${bg};border:1px solid ${border};padding:2px 8px;border-radius:4px;white-space:nowrap">` +
             `<strong>${label}</strong>: ${msg}</span>`;
    });

    const prefix = hasCritical
      ? '<span style="font-weight:700;margin-right:8px;letter-spacing:.06em">⚠ SYSTEM ALERT</span>'
      : '<span style="font-weight:700;margin-right:8px;color:#eab308;letter-spacing:.06em">ℹ CONFIG</span>';

    banner.style.display = 'flex';
    // Rebuild children (keep close button)
    banner.innerHTML = '';
    banner.appendChild(closeBtn);
    banner.insertAdjacentHTML('afterbegin', prefix + chips.join(''));
  }

  // ── Poll /api/health ───────────────────────────────────────────────────────
  async function check() {
    try {
      const ctrl = new AbortController();
      const tid  = setTimeout(() => ctrl.abort(), 12000);
      const r    = await fetch('/api/health', { signal: ctrl.signal });
      clearTimeout(tid);

      if (!r.ok) {
        render([{ check: 'server', error: `HTTP ${r.status}` }]);
        return;
      }

      const d = await r.json();
      // Reset dismiss flag on successful clear
      if (!d.errors || d.errors.length === 0) _dismissed = false;
      render(d.errors || []);
    } catch (err) {
      if (err.name === 'AbortError') {
        render([{ check: 'server', error: 'Health check timed out' }]);
      } else {
        render([{ check: 'server', error: 'Cannot reach server' }]);
      }
    }
  }

  // Initial check after 8s (let page load settle), then every 60s
  setTimeout(check, 8000);
  setInterval(() => { _dismissed = false; check(); }, 60000);
})();
