// ── Category config ──────────────────────────────────────────────────────────
const CAT_COLORS = {
  concerts:        '#4da6ff',
  performing_arts: '#c084fc',
  museum:          '#34d399',
  film:            '#fbbf24',
};

const CAT_EMOJI = {
  concerts:        '🎸',
  performing_arts: '🎭',
  museum:          '🏛',
  film:            '🎞',
};

// ── State ────────────────────────────────────────────────────────────────────
let allEvents = [];        // full dataset
let filteredEvents = [];   // after filters applied
let calendarInstance = null;
let miniMap = null;
let currentView = 'map';

const state = {
  categories: new Set(['concerts', 'performing_arts', 'museum', 'film']),
  dateRange: 'week',
  dateFrom: null,
  dateTo: null,
  search: '',
};

// ── Map setup ────────────────────────────────────────────────────────────────
const map = L.map('map', {
  center: [37.76, -122.42],
  zoom: 11,
  zoomControl: true,
  preferCanvas: true,  // faster rendering for many markers
});

L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
  attribution: '© <a href="https://openstreetmap.org/copyright">OpenStreetMap</a>',
  maxZoom: 19,
}).addTo(map);

// Marker cluster layer — replaces individual marker management
const clusterLayer = L.markerClusterGroup({
  maxClusterRadius: 50,
  disableClusteringAtZoom: 15,
  spiderfyOnMaxZoom: true,
  chunkedLoading: true,           // add markers in chunks to avoid blocking UI
  chunkInterval: 50,              // ms between chunks
  chunkDelay: 10,
});
map.addLayer(clusterLayer);

function makeIcon(category) {
  const color = CAT_COLORS[category] || '#888';
  const emoji = CAT_EMOJI[category] || '📍';
  return L.divIcon({
    className: '',
    html: `<div style="
      width:28px;height:28px;background:${color};
      border-radius:50% 50% 50% 0;transform:rotate(-45deg);
      border:2px solid rgba(255,255,255,0.5);box-shadow:0 2px 6px rgba(0,0,0,0.4);
      display:flex;align-items:center;justify-content:center;
    "><span style="transform:rotate(45deg);font-size:12px;line-height:1">${emoji}</span></div>`,
    iconSize: [28, 28],
    iconAnchor: [14, 28],
    popupAnchor: [0, -30],
  });
}

// Pre-built marker cache: venueKey → L.Marker
// Markers are reused across filter changes — only add/remove from cluster layer
const markerPool = new Map(); // key: `${lat},${lng}` → { marker, events[] }

// ── Helpers ──────────────────────────────────────────────────────────────────
function isMobile() { return window.innerWidth <= 640; }

// ── Calendar setup ───────────────────────────────────────────────────────────
function initCalendar() {
  const mobile = isMobile();
  calendarInstance = new FullCalendar.Calendar(document.getElementById('calendar'), {
    initialView: mobile ? 'listWeek' : 'dayGridMonth',
    headerToolbar: mobile
      ? { left: 'prev,next', center: 'title', right: 'listWeek,dayGridMonth' }
      : { left: 'prev,next today', center: 'title', right: 'dayGridMonth,timeGridWeek,listWeek' },
    height: mobile ? 'auto' : '100%',
    scrollTime: '14:00:00',
    slotMinTime: '10:00:00',
    slotMaxTime: '02:00:00', // next day 2am
    lazyFetching: true,
    eventDidMount: (arg) => {
      if (!arg.view.type.startsWith('list')) return;

      const ev = allEvents.find(e => e.id === arg.event.id);
      if (!ev) return;

      // The list row is a <tr>; its cells are: time | graphic | title
      // We replace the title cell's content and append venue + price cells
      const tr = arg.el.closest('tr');
      if (!tr || tr.dataset.customCols) return;
      tr.dataset.customCols = '1';

      const titleTd = tr.querySelector('.fc-list-event-title');
      if (titleTd) {
        titleTd.innerHTML = `<span class="fc-list-col-title">${escHtml(arg.event.title)}</span>`;
      }

      const loc = [ev.venue, ev.city].filter(Boolean).join(', ');

      const venueTd = document.createElement('td');
      venueTd.className = 'fc-list-col-venue-td';
      venueTd.textContent = loc;

      const priceTd = document.createElement('td');
      priceTd.className = 'fc-list-col-price-td';
      priceTd.textContent = ev.price || '';

      tr.appendChild(venueTd);
      tr.appendChild(priceTd);
    },
    eventClick: (info) => {
      const ev = allEvents.find(e => e.id === info.event.id);
      if (ev) openDetail(ev);
    },
  });
  calendarInstance.render();
}

// ── Filtering ────────────────────────────────────────────────────────────────
// Base filter: category + search (shared by map and calendar)
function baseFilter(ev) {
  if (!state.categories.has(ev.category)) return false;
  if (state.search) {
    const haystack = [ev.title, ...(ev.artists || []), ev.venue, ev.city]
      .join(' ').toLowerCase();
    if (!haystack.includes(state.search.toLowerCase())) return false;
  }
  return true;
}

// Date filter: only applied to map view; calendar handles its own date range
function dateFilter(ev) {
  if (!ev.date) return true;
  const today = new Date(); today.setHours(0, 0, 0, 0);
  const d = new Date(ev.date + 'T00:00:00');
  if (state.dateRange === 'today')  return d.toDateString() === today.toDateString();
  if (state.dateRange === 'week')   { const end = new Date(today); end.setDate(today.getDate() + 7); return d >= today && d <= end; }
  if (state.dateRange === 'month')  { const end = new Date(today.getFullYear(), today.getMonth() + 1, 0); return d >= today && d <= end; }
  if (state.dateRange === 'custom') {
    if (state.dateFrom && d < new Date(state.dateFrom)) return false;
    if (state.dateTo && d > new Date(state.dateTo + 'T23:59:59')) return false;
    return true;
  }
  return d >= today; // 'all' = upcoming only
}

function filterEvents() {
  // filteredEvents = date-scoped set (for map + counts)
  filteredEvents = allEvents.filter(ev => baseFilter(ev) && dateFilter(ev));

  updateCounts();
  document.getElementById('event-count').textContent = filteredEvents.length;
  scheduleRender();
}

// ── Debounced render scheduler ────────────────────────────────────────────────
// Renders the active view immediately; also keeps the calendar in sync so
// switching views doesn't flash stale data.
let renderPending = false;
function scheduleRender() {
  if (renderPending) return;
  renderPending = true;
  requestAnimationFrame(() => {
    renderPending = false;
    if (currentView === 'map') renderMap();
    // Always sync calendar events if it exists (cheap due to filterKey guard)
    if (calendarInstance) renderCalendar();
  });
}

function updateCounts() {
  const counts = {};
  Object.keys(CAT_COLORS).forEach(c => counts[c] = 0);
  filteredEvents.forEach(ev => { if (counts[ev.category] !== undefined) counts[ev.category]++; });
  Object.keys(counts).forEach(cat => {
    const el = document.getElementById(`count-${cat}`);
    if (el) el.textContent = counts[cat];
  });
}

// ── Map rendering ─────────────────────────────────────────────────────────────
// Groups events by lat/lng position, reuses marker pool, diffs against cluster layer
function renderMap() {
  // Build desired set of position keys → grouped events
  const desired = new Map();
  for (const ev of filteredEvents) {
    if (!ev.lat || !ev.lng) continue;
    const key = `${ev.lat.toFixed(4)},${ev.lng.toFixed(4)}`;
    if (!desired.has(key)) desired.set(key, []);
    desired.get(key).push(ev);
  }

  // Remove markers no longer in filtered set
  for (const [key, entry] of markerPool) {
    if (!desired.has(key)) {
      clusterLayer.removeLayer(entry.marker);
      markerPool.delete(key);
    }
  }

  // Add new markers; update stored group for existing ones
  const toAdd = [];
  for (const [key, group] of desired) {
    const ev = group[0];
    if (markerPool.has(key)) {
      const entry = markerPool.get(key);
      entry.events = group;
      entry.count = group.length;
    } else {
      const marker = L.marker([ev.lat, ev.lng], { icon: makeIcon(ev.category) });
      const poolKey = key; // capture for closure
      marker.on('click', () => {
        const entry = markerPool.get(poolKey);
        if (entry && entry.events.length) openDetail(entry.events[0]);
      });
      markerPool.set(key, { marker, events: group, count: group.length });
      toAdd.push(marker);
    }
  }

  if (toAdd.length > 0) clusterLayer.addLayers(toAdd);
}

// (popups removed — markers open the detail side panel directly)

// ── Calendar rendering ────────────────────────────────────────────────────────
// Uses base filter only (category + search) — FullCalendar manages its own dates.
// Tracks the last filter signature to avoid unnecessary removeAll/addAll cycles.
let lastCalendarFilterKey = '';

function renderCalendar() {
  if (!calendarInstance) return;

  // Only events matching category + search (no date restriction)
  const calendarEvents = allEvents.filter(ev => ev.date && baseFilter(ev));
  const filterKey = state.categories.size + '|' + state.search + '|' + calendarEvents.length;

  if (filterKey === lastCalendarFilterKey) return; // nothing changed
  lastCalendarFilterKey = filterKey;

  calendarInstance.removeAllEvents();
  calendarInstance.addEventSource(
    calendarEvents.slice(0, 500).map(ev => ({
      id: ev.id,
      title: ev.title,
      start: ev.date + (ev.show ? `T${to24h(ev.show)}` : ev.doors ? `T${to24h(ev.doors)}` : ''),
      backgroundColor: CAT_COLORS[ev.category] || '#888',
      borderColor: 'transparent',
      textColor: '#fff',
      extendedProps: { eventId: ev.id },
    }))
  );
}

// ── Venue grouping ───────────────────────────────────────────────────────────
// Single source of truth: given an event, find all other filtered events at the
// same venue. Used by both map marker clicks and calendar event clicks.
function getVenueGroup(ev) {
  if (!ev.venue) return [ev];
  const others = filteredEvents.filter(e => e.venue === ev.venue && e !== ev);
  return [ev, ...others];
}

// ── Detail side panel ─────────────────────────────────────────────────────────
function openDetail(ev) {
  const group = getVenueGroup(ev);
  showEventInPanel(ev, group);
}

function showEventInPanel(ev, group) {
  const panel = document.getElementById('detail-panel');
  const color = CAT_COLORS[ev.category] || '#888';
  const timeStr = ev.show
    ? `Doors ${ev.doors} / Show ${ev.show}`
    : ev.doors ? `Doors ${ev.doors}` : '—';
  const artistChips = (ev.artists || [])
    .map(a => `<span class="detail-artist-chip">${escHtml(a)}</span>`).join('');

  // Build list of other events at this location
  const others = group.filter(e => e !== ev);
  const othersHtml = others.length > 0 ? `
    <div class="detail-extra-events">
      <div class="detail-extra-title">More events here (${others.length})</div>
      ${others.map(o => `
        <div class="detail-extra-item" data-event-id="${o.id}">
          <div class="detail-extra-item-title">${escHtml(o.title)}</div>
          <div class="detail-extra-item-meta">${o.date ? formatDate(o.date) : ''}${o.price ? ' · ' + o.price : ''}</div>
        </div>
      `).join('')}
    </div>
  ` : '';

  document.getElementById('detail-content').innerHTML = `
    <div class="detail-category" style="color:${color}">${CAT_EMOJI[ev.category] || ''} ${ev.category}</div>
    <div class="detail-title">${escHtml(ev.title)}</div>
    ${artistChips ? `<div class="detail-artists">${artistChips}</div>` : ''}
    <div class="detail-info-grid">
      <div class="detail-info-item"><div class="detail-info-label">Date</div><div class="detail-info-value">${ev.date ? formatDate(ev.date) : '—'}</div></div>
      <div class="detail-info-item"><div class="detail-info-label">Time</div><div class="detail-info-value">${timeStr}</div></div>
      <div class="detail-info-item"><div class="detail-info-label">Price</div><div class="detail-info-value">${ev.price || '—'}</div></div>
      <div class="detail-info-item"><div class="detail-info-label">Age</div><div class="detail-info-value">${ev.age || '—'}</div></div>
    </div>
    <div class="detail-venue-box">
      <div class="detail-venue-name">📍 ${escHtml(ev.venue)}</div>
      ${ev.address ? `<div class="detail-venue-addr">${escHtml(ev.address)}${ev.city ? `, ${ev.city}` : ''}</div>` : ''}
      ${ev.lat && ev.lng ? `<div class="detail-mini-map" id="detail-mini-map"></div>` : ''}
    </div>
    <a class="detail-cta" href="${ev.url}" target="_blank" rel="noopener">Get tickets / more info →</a>
    ${othersHtml}
  `;

  panel.classList.remove('hidden');
  // Let the transition finish, then resize map and init mini-map
  setTimeout(() => {
    map.invalidateSize();
    initDetailMiniMap(ev);
    wireExtraItems(group);
    // On mobile, scroll the detail panel into view
    if (isMobile()) panel.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }, 280);
}

function initDetailMiniMap(ev) {
  if (miniMap) { miniMap.remove(); miniMap = null; }
  if (!ev.lat || !ev.lng) return;
  const container = document.getElementById('detail-mini-map');
  if (!container) return;
  miniMap = L.map(container, {
    center: [ev.lat, ev.lng], zoom: 15,
    zoomControl: false, attributionControl: false,
    dragging: false, scrollWheelZoom: false,
  });
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png').addTo(miniMap);
  L.marker([ev.lat, ev.lng]).addTo(miniMap);
}

function wireExtraItems(group) {
  document.querySelectorAll('.detail-extra-item').forEach(el => {
    el.addEventListener('click', () => {
      const ev = group.find(e => e.id === el.dataset.eventId);
      if (ev) {
        document.getElementById('detail-panel').scrollTop = 0;
        showEventInPanel(ev, group);
      }
    });
  });
}

function closeDetail() {
  document.getElementById('detail-panel').classList.add('hidden');
  if (miniMap) { miniMap.remove(); miniMap = null; }
  setTimeout(() => map.invalidateSize(), 280);
}

// ── Utilities ─────────────────────────────────────────────────────────────────
function escHtml(s) {
  return String(s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}
function formatDate(iso) {
  const d = new Date(iso + 'T12:00:00');
  return d.toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric' });
}
function to24h(t) {
  const m = (t || '').match(/(\d+)(?::(\d+))?\s*(am|pm)/i);
  if (!m) return '20:00:00';
  let h = parseInt(m[1]); const min = m[2] || '00'; const ap = m[3].toLowerCase();
  if (ap === 'pm' && h !== 12) h += 12;
  if (ap === 'am' && h === 12) h = 0;
  return `${String(h).padStart(2,'0')}:${min}:00`;
}

// ── Loading progress bar ─────────────────────────────────────────────────────
const TOTAL_SOURCES = 11; // number of scraper sources on server
let loadedSources = 0;

function setLoadingStatus(text, done = false) {
  const bar = document.getElementById('loading');
  const fill = document.getElementById('loading-fill');
  const textEl = document.getElementById('loading-text');

  if (textEl) textEl.textContent = text;

  if (done) {
    if (fill) { fill.classList.remove('indeterminate'); fill.style.width = '100%'; }
    setTimeout(() => {
      bar.classList.add('hidden');
      document.body.classList.add('loaded');
    }, 600);
  } else if (fill) {
    fill.classList.remove('indeterminate');
    const pct = Math.min(95, Math.round((loadedSources / TOTAL_SOURCES) * 100));
    fill.style.width = pct + '%';
  }
}

function showIndeterminate(text) {
  const bar = document.getElementById('loading');
  const fill = document.getElementById('loading-fill');
  const textEl = document.getElementById('loading-text');
  bar.classList.remove('hidden');
  document.body.classList.remove('loaded');
  if (fill) { fill.style.width = ''; fill.classList.add('indeterminate'); }
  if (textEl) textEl.textContent = text || 'Loading events...';
}

// ── Event wiring ──────────────────────────────────────────────────────────────
document.getElementById('btn-map').addEventListener('click', () => {
  currentView = 'map';
  document.getElementById('btn-map').classList.add('active');
  document.getElementById('btn-calendar').classList.remove('active');
  document.getElementById('map-view').classList.remove('hidden');
  document.getElementById('calendar-view').classList.add('hidden');
  setTimeout(() => map.invalidateSize(), 50);
  renderMap();
});

document.getElementById('btn-calendar').addEventListener('click', () => {
  currentView = 'calendar';
  document.getElementById('btn-calendar').classList.add('active');
  document.getElementById('btn-map').classList.remove('active');
  document.getElementById('calendar-view').classList.remove('hidden');
  document.getElementById('map-view').classList.add('hidden');
  if (!calendarInstance) initCalendar();
  // Force a fresh render since the container was hidden
  lastCalendarFilterKey = '';
  renderCalendar();
  calendarInstance.updateSize();
});

let searchTimer;
document.getElementById('search').addEventListener('input', (e) => {
  clearTimeout(searchTimer);
  state.search = e.target.value;
  searchTimer = setTimeout(filterEvents, 200);
});

document.querySelectorAll('.cat-filter').forEach(label => {
  label.addEventListener('click', () => {
    const cat = label.dataset.cat;
    const cb = label.querySelector('input[type=checkbox]');
    cb.checked = !cb.checked;
    cb.checked ? state.categories.add(cat) : state.categories.delete(cat);
    label.classList.toggle('disabled', !cb.checked);
    filterEvents();
  });
});

document.querySelectorAll('.date-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.date-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    state.dateRange = btn.dataset.range;
    document.getElementById('custom-dates').classList.toggle('hidden', state.dateRange !== 'custom');
    filterEvents();
  });
});

document.getElementById('date-from').addEventListener('change', (e) => { state.dateFrom = e.target.value; filterEvents(); });
document.getElementById('date-to').addEventListener('change',   (e) => { state.dateTo   = e.target.value; filterEvents(); });

document.getElementById('detail-close').addEventListener('click', closeDetail);

document.getElementById('sidebar-toggle').addEventListener('click', () => {
  document.querySelector('.sidebar').classList.toggle('collapsed');
  setTimeout(() => map.invalidateSize(), 280);
});

document.getElementById('refresh-btn').addEventListener('click', async () => {
  showIndeterminate('Refreshing events...');
  allEvents = [];
  filteredEvents = [];
  markerPool.clear();
  clusterLayer.clearLayers();
  document.getElementById('event-count').textContent = '0';
  updateCounts();
  await fetch('/api/refresh');
  await loadEvents();
});

// ── Data loading ──────────────────────────────────────────────────────────────
async function loadEvents() {
  loadedSources = 0;

  // Check if data is already cached on server
  const check = await fetch('/api/events');
  if (check.ok) {
    const data = await check.json();
    if (!data.streaming) {
      // Cache hit — got full JSON immediately
      allEvents = data;
      filterEvents();
      setLoadingStatus('', true);
      return;
    }
  }

  // No cache — stream events source by source via SSE
  showIndeterminate('Connecting to event sources...');
  await streamEvents();
}

function streamEvents() {
  return new Promise((resolve) => {
    const es = new EventSource('/api/events/stream');

    es.addEventListener('events', (e) => {
      const { source, label, events } = JSON.parse(e.data);
      if (source === 'cache') {
        allEvents = events;
        filterEvents();
        setLoadingStatus('', true);
        return;
      }
      loadedSources++;
      allEvents.push(...events);
      setLoadingStatus(`${label || source} loaded (${allEvents.length} events)`);
      filterEvents();
    });

    es.addEventListener('error', (e) => {
      try {
        const { source, message } = JSON.parse(e.data || '{}');
        console.warn(`Source ${source} failed:`, message);
        loadedSources++;
        setLoadingStatus(`${loadedSources} of ${TOTAL_SOURCES} sources loaded`);
      } catch (_) {}
    });

    es.addEventListener('done', (e) => {
      const { total } = JSON.parse(e.data);
      setLoadingStatus(`${total} events loaded`, true);
      es.close();
      resolve();
    });

    es.onerror = () => {
      setLoadingStatus('Connection error — showing loaded events', true);
      es.close();
      resolve();
    };
  });
}

loadEvents();
