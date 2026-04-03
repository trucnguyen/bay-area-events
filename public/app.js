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

// ── Calendar setup ───────────────────────────────────────────────────────────
function initCalendar() {
  calendarInstance = new FullCalendar.Calendar(document.getElementById('calendar'), {
    initialView: 'dayGridMonth',
    headerToolbar: {
      left: 'prev,next today',
      center: 'title',
      right: 'dayGridMonth,timeGridWeek,listWeek',
    },
    height: '100%',
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
      if (ev) openModal(ev);
    },
  });
  calendarInstance.render();
}

// ── Filtering ────────────────────────────────────────────────────────────────
function filterEvents() {
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  const endOfWeek = new Date(today); endOfWeek.setDate(today.getDate() + 7);
  const endOfMonth = new Date(today.getFullYear(), today.getMonth() + 1, 0);
  const searchLower = state.search.toLowerCase();

  filteredEvents = allEvents.filter(ev => {
    if (!state.categories.has(ev.category)) return false;

    if (searchLower) {
      const haystack = [ev.title, ...(ev.artists || []), ev.venue, ev.city]
        .join(' ').toLowerCase();
      if (!haystack.includes(searchLower)) return false;
    }

    if (ev.date) {
      const d = new Date(ev.date + 'T00:00:00');
      if (state.dateRange === 'today')  return d.toDateString() === today.toDateString();
      if (state.dateRange === 'week')   return d >= today && d <= endOfWeek;
      if (state.dateRange === 'month')  return d >= today && d <= endOfMonth;
      if (state.dateRange === 'custom') {
        if (state.dateFrom && d < new Date(state.dateFrom)) return false;
        if (state.dateTo && d > new Date(state.dateTo + 'T23:59:59')) return false;
      } else {
        if (d < today) return false; // 'all' = upcoming only
      }
    }
    return true;
  });

  updateCounts();
  document.getElementById('event-count').textContent = filteredEvents.length;
  scheduleRender();
}

// ── Debounced render scheduler ────────────────────────────────────────────────
// Only renders the active view, and defers via requestAnimationFrame
let renderPending = false;
function scheduleRender() {
  if (renderPending) return;
  renderPending = true;
  requestAnimationFrame(() => {
    renderPending = false;
    if (currentView === 'map') renderMap();
    else renderCalendar();
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

  // Add new markers; update popup for existing ones whose event group changed
  const toAdd = [];
  for (const [key, group] of desired) {
    const ev = group[0];
    if (markerPool.has(key)) {
      // Update popup content if group changed
      const entry = markerPool.get(key);
      if (entry.count !== group.length) {
        entry.marker.setPopupContent(buildPopup(group));
        entry.count = group.length;
      }
    } else {
      const marker = L.marker([ev.lat, ev.lng], { icon: makeIcon(ev.category) });
      marker.bindPopup(buildPopup(group), { maxWidth: 300 });
      markerPool.set(key, { marker, count: group.length });
      toAdd.push(marker);
    }
  }

  if (toAdd.length > 0) clusterLayer.addLayers(toAdd);
}

function buildPopup(group) {
  const ev = group[0];
  const color = CAT_COLORS[ev.category] || '#888';
  const artistList = (ev.artists || []).slice(0, 4).join(', ');
  const timeStr = ev.show ? `${ev.doors} / ${ev.show}` : ev.doors || '';
  const meta = [
    ev.date  ? `📅 ${formatDate(ev.date)}` : '',
    timeStr  ? `🕐 ${timeStr}` : '',
    ev.price ? `💵 ${ev.price}` : '',
    ev.age && ev.age !== 'all ages' ? ev.age : '',
  ].filter(Boolean).map(t => `<span class="popup-tag">${t}</span>`).join('');
  const extra = group.length > 1
    ? `<div style="color:var(--text-muted);font-size:11px;margin-top:4px">+${group.length - 1} more event${group.length > 2 ? 's' : ''} this day</div>`
    : '';
  return `<div class="popup-inner">
    <div class="popup-category" style="color:${color}">${CAT_EMOJI[ev.category] || ''} ${ev.category}</div>
    <div class="popup-title">${escHtml(ev.title)}</div>
    ${artistList ? `<div class="popup-artists">${escHtml(artistList)}</div>` : ''}
    <div class="popup-meta">${meta}</div>
    <div class="popup-venue">📍 ${escHtml(ev.venue)}${ev.city ? `, ${ev.city}` : ''}</div>
    ${extra}
    <a class="popup-link" href="${ev.url}" target="_blank" rel="noopener">More info →</a>
  </div>`;
}

// ── Calendar rendering ────────────────────────────────────────────────────────
// Batched via addEventSource for large sets; only rebuilds when view is active
function renderCalendar() {
  if (!calendarInstance) return;
  calendarInstance.removeAllEvents();
  // Batch add up to 500 events (calendar becomes slow beyond that)
  const eventsToShow = filteredEvents.filter(e => e.date).slice(0, 500);
  calendarInstance.addEventSource(
    eventsToShow.map(ev => ({
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

// ── Modal ─────────────────────────────────────────────────────────────────────
function openModal(ev) {
  const color = CAT_COLORS[ev.category] || '#888';
  const timeStr = ev.show
    ? `Doors ${ev.doors} / Show ${ev.show}`
    : ev.doors ? `Doors ${ev.doors}` : '—';
  const artistChips = (ev.artists || [])
    .map(a => `<span class="modal-artist-chip">${escHtml(a)}</span>`).join('');

  document.getElementById('modal-content').innerHTML = `
    <div class="modal-category" style="color:${color}">${CAT_EMOJI[ev.category] || ''} ${ev.category}</div>
    <div class="modal-title">${escHtml(ev.title)}</div>
    ${artistChips ? `<div class="modal-artists">${artistChips}</div>` : ''}
    <div class="modal-info-grid">
      <div class="modal-info-item"><div class="modal-info-label">Date</div><div class="modal-info-value">${ev.date ? formatDate(ev.date) : '—'}</div></div>
      <div class="modal-info-item"><div class="modal-info-label">Time</div><div class="modal-info-value">${timeStr}</div></div>
      <div class="modal-info-item"><div class="modal-info-label">Price</div><div class="modal-info-value">${ev.price || '—'}</div></div>
      <div class="modal-info-item"><div class="modal-info-label">Age</div><div class="modal-info-value">${ev.age || '—'}</div></div>
    </div>
    <div class="modal-venue-box">
      <div class="modal-venue-name">📍 ${escHtml(ev.venue)}</div>
      ${ev.address ? `<div class="modal-venue-addr">${escHtml(ev.address)}${ev.city ? `, ${ev.city}` : ''}</div>` : ''}
      ${ev.lat && ev.lng ? `<div class="modal-mini-map" id="modal-mini-map"></div>` : ''}
    </div>
    <a class="modal-cta" id="modal-cta-link" href="${ev.url}" target="_blank" rel="noopener">Get tickets / more info →</a>
  `;

  document.getElementById('modal-overlay').classList.remove('hidden');

  // For TheList concerts, try to resolve a direct Songkick event page
  if (ev.id && ev.id.startsWith('thelist-') && ev.venueSlug) {
    fetch(`/api/resolve-url?artist=${encodeURIComponent(ev.title)}&venue=${encodeURIComponent(ev.venueSlug)}&date=${encodeURIComponent(ev.date || '')}`)
      .then(r => r.json())
      .then(({ url }) => {
        if (!url) return;
        const link = document.getElementById('modal-cta-link');
        if (link) link.href = url;
      })
      .catch(() => {});
  }

  if (ev.lat && ev.lng) {
    setTimeout(() => {
      const container = document.getElementById('modal-mini-map');
      if (!container) return;
      if (miniMap) { miniMap.remove(); miniMap = null; }
      miniMap = L.map(container, {
        center: [ev.lat, ev.lng], zoom: 15,
        zoomControl: false, attributionControl: false,
        dragging: false, scrollWheelZoom: false,
      });
      L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png').addTo(miniMap);
      L.marker([ev.lat, ev.lng]).addTo(miniMap);
    }, 50);
  }
}

function closeModal() {
  document.getElementById('modal-overlay').classList.add('hidden');
  if (miniMap) { miniMap.remove(); miniMap = null; }
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

// ── Loading status bar ────────────────────────────────────────────────────────
function setLoadingStatus(text, done = false) {
  const el = document.querySelector('.loading-text');
  if (el) el.textContent = text;
  if (done) {
    setTimeout(() => document.getElementById('loading').classList.add('hidden'), 400);
  }
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
  else calendarInstance.render();
  renderCalendar();
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

document.getElementById('modal-close').addEventListener('click', closeModal);
document.getElementById('modal-overlay').addEventListener('click', (e) => {
  if (e.target === document.getElementById('modal-overlay')) closeModal();
});

document.getElementById('refresh-btn').addEventListener('click', async () => {
  document.getElementById('loading').classList.remove('hidden');
  setLoadingStatus('Refreshing events...');
  allEvents = [];
  markerPool.clear();
  clusterLayer.clearLayers();
  await fetch('/api/refresh');
  await loadEvents();
});

// ── Data loading ──────────────────────────────────────────────────────────────
async function loadEvents() {
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
  setLoadingStatus('Loading concerts...');
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
      allEvents.push(...events);
      setLoadingStatus(`Loading… (${allEvents.length} events so far)`);
      // Immediately apply filters and render new events
      filterEvents();
    });

    es.addEventListener('error', (e) => {
      const { source, message } = JSON.parse(e.data || '{}');
      console.warn(`Source ${source} failed:`, message);
    });

    es.addEventListener('done', (e) => {
      const { total } = JSON.parse(e.data);
      setLoadingStatus('', true);
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
