import express from 'express';
import cors from 'cors';
import fetch from 'node-fetch';
import * as cheerio from 'cheerio';
import NodeCache from 'node-cache';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import { readFileSync, writeFileSync, existsSync } from 'fs';
import { execSync } from 'child_process';

const __dirname = dirname(fileURLToPath(import.meta.url));
const app = express();
const cache = new NodeCache({ stdTTL: 1800 }); // 30 min in-memory TTL

const DISK_CACHE_PATH = join(__dirname, 'cache.json');
const DISK_CACHE_TTL_MS = 30 * 60 * 1000; // 30 minutes

function loadDiskCache() {
  try {
    if (!existsSync(DISK_CACHE_PATH)) return;
    const { ts, events } = JSON.parse(readFileSync(DISK_CACHE_PATH, 'utf-8'));
    if (Date.now() - ts < DISK_CACHE_TTL_MS) {
      cache.set('events', events);
      console.log(`Loaded ${events.length} events from disk cache (age: ${Math.round((Date.now()-ts)/60000)}min)`);
    }
  } catch (e) {
    console.warn('Disk cache load failed:', e.message);
  }
}

function saveDiskCache(events) {
  try {
    writeFileSync(DISK_CACHE_PATH, JSON.stringify({ ts: Date.now(), events }));
  } catch (e) {
    console.warn('Disk cache save failed:', e.message);
  }
}

loadDiskCache();

app.use(cors());
app.use(express.static(join(__dirname, 'public')));

// ─── Venue coordinate lookup table ───────────────────────────────────────────
const VENUE_COORDS = {
  // SF Venues
  'great_american_music_hall':  { lat: 37.7842, lng: -122.4178, address: '859 O\'Farrell St', city: 'SF' },
  'fillmore':                   { lat: 37.7842, lng: -122.4328, address: '1805 Geary Blvd', city: 'SF' },
  'warfield':                   { lat: 37.7826, lng: -122.4097, address: '982 Market St', city: 'SF' },
  'independent':                { lat: 37.7762, lng: -122.4376, address: '628 Divisadero St', city: 'SF' },
  'chapel':                     { lat: 37.7629, lng: -122.4209, address: '777 Valencia St', city: 'SF' },
  'bottom_of_the_hill':         { lat: 37.7627, lng: -122.3971, address: '1233 17th St', city: 'SF' },
  'brick_and_mortar':           { lat: 37.7695, lng: -122.4192, address: '1710 Mission St', city: 'SF' },
  'cafe_du_nord':               { lat: 37.7654, lng: -122.4330, address: '2170 Market St', city: 'SF' },
  'august_hall':                { lat: 37.7868, lng: -122.4085, address: '420 Mason St', city: 'SF' },
  'rickshaw_stop':              { lat: 37.7773, lng: -122.4218, address: '155 Fell St', city: 'SF' },
  'rickshaw_shop':              { lat: 37.7773, lng: -122.4218, address: '155 Fell St', city: 'SF' },
  'slim\'s':                    { lat: 37.7703, lng: -122.4115, address: '333 11th St', city: 'SF' },
  'dna_lounge':                 { lat: 37.7695, lng: -122.4114, address: '375 11th St', city: 'SF' },
  'black_cat':                  { lat: 37.7838, lng: -122.4155, address: '400 Eddy St', city: 'SF' },
  'boom_boom_room':             { lat: 37.7835, lng: -122.4329, address: '1601 Fillmore St', city: 'SF' },
  'bimbo\'s_365_club':          { lat: 37.8003, lng: -122.4098, address: '1025 Columbus Ave', city: 'SF' },
  'Castro':                     { lat: 37.7622, lng: -122.4350, address: '429 Castro St', city: 'SF' },
  'castro_theater':             { lat: 37.7622, lng: -122.4350, address: '429 Castro St', city: 'SF' },
  'civic_auditorium':           { lat: 37.7782, lng: -122.4192, address: '99 Grove St', city: 'SF' },
  'davies_symphony_hall':       { lat: 37.7774, lng: -122.4196, address: '201 Van Ness Ave', city: 'SF' },
  'herbst_theatre':             { lat: 37.7785, lng: -122.4194, address: '401 Van Ness Ave', city: 'SF' },
  'war_memorial_opera_house':   { lat: 37.7772, lng: -122.4203, address: '301 Van Ness Ave', city: 'SF' },
  'act_geary_theater':          { lat: 37.7866, lng: -122.4088, address: '415 Geary St', city: 'SF' },
  'sfmoma':                     { lat: 37.7857, lng: -122.4011, address: '151 3rd St', city: 'SF' },
  'de_young_museum':            { lat: 37.7714, lng: -122.4686, address: '50 Hagiwara Tea Garden Dr', city: 'SF' },
  'legion_of_honor':            { lat: 37.7863, lng: -122.5027, address: '100 34th Ave', city: 'SF' },
  'palace_of_fine_arts':        { lat: 37.8029, lng: -122.4484, address: '3601 Lyon St', city: 'SF' },
  'american_indian_museum':     { lat: 37.7967, lng: -122.4026, address: '631 Howard St', city: 'SF' },
  'yoshi\'s':                   { lat: 37.7991, lng: -122.2735, address: '510 Embarcadero W', city: 'Oakland' },
  // Oakland/East Bay
  'fox_theater':                { lat: 37.8074, lng: -122.2689, address: '1807 Telegraph Ave', city: 'Oakland' },
  'paramount_theatre':          { lat: 37.8076, lng: -122.2693, address: '2025 Broadway', city: 'Oakland' },
  'greek_theatre':              { lat: 37.8720, lng: -122.2593, address: '2001 Gayley Rd', city: 'Berkeley' },
  'gilman':                     { lat: 37.8688, lng: -122.2757, address: '924 Gilman St', city: 'Berkeley' },
  'cornerstone':                { lat: 37.8630, lng: -122.2606, address: '2367 Shattuck Ave', city: 'Berkeley' },
  'freight':                    { lat: 37.8694, lng: -122.2680, address: '2020 Addison St', city: 'Berkeley' },
  'ivy_room':                   { lat: 37.8927, lng: -122.3017, address: '860 San Pablo Ave', city: 'Albany' },
  'crybaby':                    { lat: 37.8067, lng: -122.2725, address: '1928 Telegraph Ave', city: 'Oakland' },
  'bampfa':                     { lat: 37.8688, lng: -122.2600, address: '2155 Center St', city: 'Berkeley' },
  // South Bay / Peninsula
  'stanford_theater':           { lat: 37.4457, lng: -122.1611, address: '221 University Ave', city: 'Palo Alto' },
  'frost_amphitheater':         { lat: 37.4312, lng: -122.1671, address: 'Serra Mall', city: 'Stanford' },
  'shoreline_amphitheatre':     { lat: 37.4269, lng: -122.0810, address: '1 Amphitheatre Pkwy', city: 'Mountain View' },
  'san_jose_civic':             { lat: 37.3333, lng: -121.8894, address: '135 W San Carlos St', city: 'San Jose' },
  'city_national_civic':        { lat: 37.3333, lng: -121.8894, address: '135 W San Carlos St', city: 'San Jose' },
  // Marin / North Bay
  'sweetwater_music_hall':      { lat: 37.9724, lng: -122.5311, address: '19 Corte Madera Ave', city: 'Mill Valley' },
  'music_in_the_hills':         { lat: 37.9300, lng: -122.5200, address: 'Marin', city: 'Marin' },
  // More SF
  'regency_ballroom':           { lat: 37.7836, lng: -122.4155, address: '1300 Van Ness Ave', city: 'SF' },
  'masonic':                    { lat: 37.7882, lng: -122.4262, address: '1111 California St', city: 'SF' },
  'sf_masonic':                 { lat: 37.7882, lng: -122.4262, address: '1111 California St', city: 'SF' },
  'the_masonic':                { lat: 37.7882, lng: -122.4262, address: '1111 California St', city: 'SF' },
  'knockout':                   { lat: 37.7568, lng: -122.4208, address: '3223 Mission St', city: 'SF' },
  'kilowatt':                   { lat: 37.7657, lng: -122.4230, address: '3160 16th St', city: 'SF' },
  'thee_parkside':              { lat: 37.7560, lng: -122.4030, address: '1600 17th St', city: 'SF' },
  'parkside':                   { lat: 37.7560, lng: -122.4030, address: '1600 17th St', city: 'SF' },
  'ritz':                       { lat: 37.7783, lng: -122.4158, address: '2nd and Mission', city: 'SF' },
  'the_ritz':                   { lat: 37.7783, lng: -122.4158, address: '2nd and Mission', city: 'SF' },
  'planetarium':                { lat: 37.7693, lng: -122.4660, address: '55 Music Concourse Dr', city: 'SF' },
  'golden_gate_park_bandshell':{ lat: 37.7706, lng: -122.4538, address: 'Golden Gate Park', city: 'SF' },
  'speakeasy_ales':             { lat: 37.7574, lng: -122.4125, address: '1195 Evans Ave', city: 'SF' },
  'phoenix_theater':            { lat: 37.7732, lng: -122.4164, address: '462 Pine St', city: 'SF' },
  'the_phoenix':                { lat: 37.7732, lng: -122.4164, address: '462 Pine St', city: 'SF' },
  'slim\'s_sf':                 { lat: 37.7703, lng: -122.4115, address: '333 11th St', city: 'SF' },
  'sf_jazz_center':             { lat: 37.7770, lng: -122.4220, address: '201 Franklin St', city: 'SF' },
  'jazz_center':                { lat: 37.7770, lng: -122.4220, address: '201 Franklin St', city: 'SF' },
  'herbst':                     { lat: 37.7785, lng: -122.4194, address: '401 Van Ness Ave', city: 'SF' },
  'mcl_at_city_hall':           { lat: 37.7793, lng: -122.4193, address: 'SF City Hall', city: 'SF' },
  'bar_fluxus':                 { lat: 37.7867, lng: -122.4068, address: '584 Sutter St', city: 'SF' },
  'the_royal_cuckoo':           { lat: 37.7620, lng: -122.4296, address: '3202 Mission St', city: 'SF' },
  'natural_batting_cage':       { lat: 37.7632, lng: -122.4225, address: '3255 18th St', city: 'SF' },
  // Oakland / East Bay extended
  'thee_stork_club':            { lat: 37.8088, lng: -122.2673, address: '2330 Telegraph Ave', city: 'Oakland' },
  'stork_club':                 { lat: 37.8088, lng: -122.2673, address: '2330 Telegraph Ave', city: 'Oakland' },
  'starline_social_club':       { lat: 37.8104, lng: -122.2661, address: '2236 Martin Luther King Jr Way', city: 'Oakland' },
  'uptown_theater':             { lat: 37.8096, lng: -122.2690, address: '1928 Telegraph Ave', city: 'Oakland' },
  'uptown':                     { lat: 37.8096, lng: -122.2690, address: '1928 Telegraph Ave', city: 'Oakland' },
  'new_parish':                 { lat: 37.8107, lng: -122.2789, address: '579 18th St', city: 'Oakland' },
  'the_new_parish':             { lat: 37.8107, lng: -122.2789, address: '579 18th St', city: 'Oakland' },
  'east_bay_express':           { lat: 37.8040, lng: -122.2720, address: 'Oakland', city: 'Oakland' },
  'henry_j_kaiser_auditorium':  { lat: 37.8050, lng: -122.2625, address: '10 10th St', city: 'Oakland' },
  'schnitzer_concert_hall':     { lat: 37.8096, lng: -122.2756, address: 'Oakland', city: 'Oakland' },
  'guild_theater':              { lat: 37.7955, lng: -122.2333, address: '3814 Grand Ave', city: 'Oakland' },
  'the_guild':                  { lat: 37.7955, lng: -122.2333, address: '3814 Grand Ave', city: 'Oakland' },
  'african_american_museum':    { lat: 37.8040, lng: -122.2710, address: 'Oakland Museum', city: 'Oakland' },
  // Berkeley extended
  'uc_theater':                 { lat: 37.8694, lng: -122.2688, address: '2036 University Ave', city: 'Berkeley' },
  'the_uc_theater':             { lat: 37.8694, lng: -122.2688, address: '2036 University Ave', city: 'Berkeley' },
  'zellerbach_hall':            { lat: 37.8730, lng: -122.2591, address: 'UC Berkeley Campus', city: 'Berkeley' },
  'berkeley_symphony':          { lat: 37.8694, lng: -122.2680, address: '2020 Addison St', city: 'Berkeley' },
  // Santa Cruz extended
  'moe\'s_alley':               { lat: 36.9906, lng: -122.0524, address: '1535 Commercial Way', city: 'Santa Cruz' },
  'rio_theater':                { lat: 36.9783, lng: -122.0241, address: '1205 Soquel Ave', city: 'Santa Cruz' },
  'up_the_creek':               { lat: 37.0490, lng: -121.9870, address: 'Scotts Valley', city: 'Scotts Valley' },
  // Wine country / Napa
  'hopmonk_tavern':             { lat: 38.4015, lng: -122.8553, address: '230 Petaluma Ave', city: 'Sebastopol' },
  'hopmonk':                    { lat: 38.4015, lng: -122.8553, address: '230 Petaluma Ave', city: 'Sebastopol' },
  'meritage_resort':            { lat: 38.2959, lng: -122.3193, address: '875 Bordeaux Way', city: 'Napa' },
  // Other
  'mountain_winery':            { lat: 37.2625, lng: -121.9995, address: '14831 Pierce Rd', city: 'Saratoga' },
  'montalvo_arts_center':       { lat: 37.2622, lng: -122.0037, address: '15400 Montalvo Rd', city: 'Saratoga' },
  'warriors_stadium':           { lat: 37.7680, lng: -122.3874, address: 'Chase Center, 1 Warriors Way', city: 'SF' },
  'chase_center':               { lat: 37.7680, lng: -122.3874, address: '1 Warriors Way', city: 'SF' },
  'oracle_arena':               { lat: 37.7504, lng: -122.2030, address: '7000 Coliseum Way', city: 'Oakland' },
  'oakland_arena':              { lat: 37.7504, lng: -122.2030, address: '7000 Coliseum Way', city: 'Oakland' },
  'shoreline_amphitheatre_mv':  { lat: 37.4269, lng: -122.0810, address: '1 Amphitheatre Pkwy', city: 'Mountain View' },
};

// Arts venue fixed locations (for scrapers that don't return coords)
const ARTS_VENUE_COORDS = {
  'SF Ballet':          { lat: 37.7772, lng: -122.4203, address: '301 Van Ness Ave', city: 'SF' },
  'SF Opera':           { lat: 37.7772, lng: -122.4203, address: '301 Van Ness Ave', city: 'SF' },
  'ACT':                { lat: 37.7866, lng: -122.4088, address: '415 Geary St', city: 'SF' },
  'SFMOMA':             { lat: 37.7857, lng: -122.4011, address: '151 3rd St', city: 'SF' },
  'BAMPFA':             { lat: 37.8688, lng: -122.2600, address: '2155 Center St', city: 'Berkeley' },
  'Stanford Theater':   { lat: 37.4457, lng: -122.1611, address: '221 University Ave', city: 'Palo Alto' },
  'de Young':           { lat: 37.7714, lng: -122.4686, address: '50 Hagiwara Tea Garden Dr', city: 'SF' },
  'Legion of Honor':    { lat: 37.7863, lng: -122.5027, address: '100 34th Ave', city: 'SF' },
  'Asian Art Museum':   { lat: 37.7806, lng: -122.4162, address: '200 Larkin St', city: 'SF' },
  'Oakland Museum':     { lat: 37.7986, lng: -122.2643, address: '1000 Oak St', city: 'Oakland' },
  'YBCA':               { lat: 37.7849, lng: -122.4025, address: '701 Mission St', city: 'SF' },
  'Orpheum Theatre':    { lat: 37.7793, lng: -122.4137, address: '1192 Market St', city: 'SF' },
  'Golden Gate Theatre': { lat: 37.7820, lng: -122.4108, address: '1 Taylor St', city: 'SF' },
  'Curran Theatre':     { lat: 37.7866, lng: -122.4104, address: '445 Geary St', city: 'SF' },
};

// Nominatim geocode cache (in-memory, persists for server lifetime)
const geocodeCache = {};

async function geocodeVenue(venueName, city = '') {
  const key = `${venueName}|${city}`;
  if (geocodeCache[key]) return geocodeCache[key];
  try {
    const q = encodeURIComponent(`${venueName} ${city} Bay Area California`);
    const url = `https://nominatim.openstreetmap.org/search?format=json&limit=1&q=${q}`;
    const res = await fetch(url, { headers: { 'User-Agent': 'BayAreaEventsApp/1.0' } });
    const data = await res.json();
    if (data.length > 0) {
      const coords = { lat: parseFloat(data[0].lat), lng: parseFloat(data[0].lon) };
      geocodeCache[key] = coords;
      return coords;
    }
  } catch (e) { /* silently fail */ }
  return null;
}

function getVenueCoords(slug) {
  const key = slug.toLowerCase().replace(/[^a-z0-9_]/g, '_');
  return VENUE_COORDS[key] || VENUE_COORDS[slug] || null;
}

// ─── Venue website URL map (keyed by TheList slug) ───────────────────────────
const VENUE_URLS = {
  'fillmore':                 'https://www.thefillmore.com/events',
  'warfield':                 'https://www.thewarfield.com/events',
  'independent':              'https://www.theindependentsf.com/calendar',
  'great_american_music_hall':'https://www.gamh.com/events',
  'chapel':                   'https://www.thechapelsf.com/calendar',
  'bottom_of_the_hill':       'https://www.bottomofthehill.com/calendar.html',
  'brick_and_mortar':         'https://www.brickandmortarmusic.com/events',
  'cafe_du_nord':             'https://www.cafedunord.com/calendar',
  'swedish_american_hall':    'https://www.cafedunord.com/calendar',
  'dna_lounge':               'https://www.dnalounge.com/calendar/',
  'slim_s':                   'https://www.slims.net/calendar',
  'great_northern':           'https://www.greatnorthernsf.com/events',
  'august_hall':              'https://www.augusthallsf.com/events',
  'regency_ballroom':         'https://www.theregencyballroom.com/events',
  'masonic':                  'https://www.themasonic.com/calendar',
  'castro_theater':           'https://thecastro.com/events',
  'castro_theataer':          'https://thecastro.com/events',
  'golden_gate_theater':      'https://www.shnsf.com/shows',
  'davies_symphony_hall':     'https://www.sfsymphony.org/Buy-Tickets/Calendar',
  'palace_of_fine_arts':      'https://palaceoffinearts.org/events/',
  'rickshaw_stop':            'https://rickshawstop.com/calendar',
  'rickshas_stop':            'https://rickshawstop.com/calendar',
  'rickshaw_shop':            'https://rickshawstop.com/calendar',
  'make-out_room':            'https://www.makeoutroom.com',
  'knockout':                 'https://theknockoutsf.com/calendar',
  'thee_stork_club':          'https://theestorkclub.com/calendar',
  'eli_s_mile_high_club':     'https://www.elismilehighclub.com/events',
  'eli\'s_mile_high_club':    'https://www.elismilehighclub.com/events',
  'yoshi_s':                  'https://yoshis.com/calendar',
  'yoshi\'s':                 'https://yoshis.com/calendar',
  'freight':                  'https://www.freightandsalvage.org/calendar/',
  'uc_theater':               'https://theuctheater.com/events/',
  'greek_theatre':            'https://www.ucgreektheatre.com/events',
  'fox_theater':              'https://www.foxoakland.com/events',
  'fox_theataer':             'https://www.foxoakland.com/events',
  'paramount_theatre':        'https://www.paramounttheatre.com/events',
  'guild_theater':            'https://guildtheater.com/events',
  'ivy_room':                 'https://ivyroomalbany.com/events',
  'uptown_theater':           'https://www.uptownclubkc.com/events',
  'catalyst':                 'https://www.catalystclub.com/events',
  'catalyst_atrium':          'https://www.catalystclub.com/events',
  'crepe_place':              'https://www.thecrepeplace.com/shows',
  'moe_s_alley':              'https://moesalley.com/calendar/',
  'moe\'s_alley':             'https://moesalley.com/calendar/',
  'rio_theater':              'https://www.riotheatre.com/events',
  'sweetwater_music_hall':    'https://www.sweetwatermusichall.com/events',
  'mystic_theater':           'https://mystictheatre.com/events',
  'hopmonk':                  'https://www.hopmonk.com/events',
  'hopmonk_tavern':           'https://www.hopmonk.com/events',
  'gray_area':                'https://grayarea.org/events/',
  'stern_grove':              'https://www.sterngrove.org/concerts/',
  'outside_lands':            'https://www.sfoutsidelands.com/',
  'shoreline_amphitheatre':   'https://www.livenation.com/venue/KovZpZAE6lnA/shoreline-amphitheatre-events',
  'shoreline_ampheater':      'https://www.livenation.com/venue/KovZpZAE6lnA/shoreline-amphitheatre-events',
  'shoreline_amphteater':     'https://www.livenation.com/venue/KovZpZAE6lnA/shoreline-amphitheatre-events',
  'shorline_amphitheater':    'https://www.livenation.com/venue/KovZpZAE6lnA/shoreline-amphitheatre-events',
  'mountain_winery':          'https://www.mountainwinery.com/events',
  'frost_amphitheater':       'https://frostamphitheater.com/events',
  'luther_burbank_center':    'https://lutherburbankcenter.org/events/',
  'san_jose_civic':           'https://www.sanjosecivic.com/events',
  'san_jose_civic_center':    'https://www.sanjosecivic.com/events',
  'hammer_theater_center':    'https://www.hammertheatre.com/events',
};

function getVenueUrl(slug) {
  const key = slug.toLowerCase().replace(/[^a-z0-9_']/g, '_');
  return VENUE_URLS[slug] || VENUE_URLS[key] || null;
}

// ─── Songkick search ─────────────────────────────────────────────────────────
const skCache = new NodeCache({ stdTTL: 86400 });

async function searchSongkick(artist, venueSlug) {
  const cacheKey = `sk:${artist}:${venueSlug}`.toLowerCase();
  const cached = skCache.get(cacheKey);
  if (cached !== undefined) return cached;

  const venueName = SONGKICK_VENUE_NAMES[venueSlug] || venueSlug.replace(/_/g, ' ');
  const query = encodeURIComponent(`${artist} ${venueName}`);
  try {
    const html = await fetch(`https://www.songkick.com/search?query=${query}`, {
      headers: { 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36' }
    }).then(r => r.text());

    const matches = [...html.matchAll(/href="(\/concerts\/\d+-[^"]+)"/g)].map(m => m[1]);
    const venueWords = venueName.toLowerCase().split(/\s+/).filter(w => w.length > 3);

    let best = null;
    for (const href of matches) {
      if (venueWords.some(w => href.toLowerCase().includes(w))) {
        best = `https://www.songkick.com${href}`;
        break;
      }
    }
    skCache.set(cacheKey, best);
    return best;
  } catch {
    skCache.set(cacheKey, null);
    return null;
  }
}

async function enrichSongkickUrls(events) {
  // Try to find artist-specific permalinks for all TheList events.
  // Events with only a generic venue calendar URL (from VENUE_URLS) often 404
  // because /events isn't always a valid path. Songkick gives us direct links.
  const toEnrich = events.filter(ev => ev.venueSlug);
  if (toEnrich.length === 0) return;

  console.log(`Enriching ${toEnrich.length} events with Songkick URLs...`);
  const BATCH = 5;
  for (let i = 0; i < toEnrich.length; i += BATCH) {
    await Promise.all(
      toEnrich.slice(i, i + BATCH).map(async ev => {
        const url = await searchSongkick(ev.title, ev.venueSlug);
        if (url) {
          ev.url = url;
        } else if (ev.url === 'https://jon.luini.com/thelist/date.html') {
          // No Songkick result and no venue URL — fall back to venue calendar page
          const venueUrl = getVenueUrl(ev.venueSlug);
          if (venueUrl) ev.url = venueUrl;
        }
      })
    );
  }
  console.log('Songkick enrichment complete.');
}

// ─── TheList scraper ──────────────────────────────────────────────────────────
// Uses regex-based parsing to handle the mixed single/double quote HTML reliably.
async function scrapeTheList() {
  const events = [];
  try {
    const [dateHtml, clubHtml] = await Promise.all([
      fetch('https://jon.luini.com/thelist/date.html').then(r => r.text()),
      fetch('https://jon.luini.com/thelist/club.html').then(r => r.text()),
    ]);

    // Build venue address map from club.html
    // Pattern: <A NAME="slug"><B>Name</B></A> then next TD has address
    const venueMap = {};
    const clubRows = clubHtml.split(/<\/tr>/i);
    for (const row of clubRows) {
      const slugMatch = row.match(/<a\s+name="([^"]+)"/i);
      if (!slugMatch) continue;
      const slug = slugMatch[1];
      // Get the second TD content in this row (address)
      const tds = row.match(/<td[^>]*>([\s\S]*?)<\/td>/gi) || [];
      if (tds.length >= 2) {
        const addrRaw = tds[1].replace(/<[^>]+>/g, '').trim();
        venueMap[slug] = addrRaw;
      }
    }

    // Parse date.html using row-level regex
    const MONTHS = ['january','february','march','april','may','june',
      'july','august','september','october','november','december'];

    let currentMonth = '';
    let currentYear = new Date().getFullYear();
    let currentDate = null;

    // Split into rows
    const rows = dateHtml.split(/<\/tr>/i);

    for (const row of rows) {
      // Month header: <TH COLSPAN=4 BGCOLOR='#FFCC00'><FONT SIZE=4>april 2026</FONT></TH>
      const monthMatch = row.match(/bgcolor=['"]#FFCC00['"]/i);
      if (monthMatch) {
        const textMatch = row.match(/<font[^>]*>([\w\s]+)<\/font>/i);
        if (textMatch) {
          const parts = textMatch[1].trim().toLowerCase().split(/\s+/);
          if (parts.length === 2 && MONTHS.includes(parts[0])) {
            currentMonth = parts[0];
            currentYear = parseInt(parts[1], 10);
          }
        }
        continue;
      }

      // Date cell: <TD ROWSPAN=N ... BGCOLOR="#CCCC00"> contains day number
      const dateCellMatch = row.match(/bgcolor=["']#CCCC00["']/i);
      if (dateCellMatch && currentMonth) {
        const dayMatch = row.match(/<b>(?:\w+<br\s*\/?>)?(\d+)<\/b>/i);
        if (dayMatch) {
          const day = parseInt(dayMatch[1], 10);
          const monthIdx = MONTHS.indexOf(currentMonth);
          if (monthIdx >= 0) {
            currentDate = new Date(currentYear, monthIdx, day);
          }
        }
      }

      if (!currentDate) continue;

      // Event row: must have exactly 3 TDs with alternating gray/white bg
      // and NOT be a date/month header row
      if (dateCellMatch || monthMatch) continue;

      // Extract all TDs
      const tdMatches = [...row.matchAll(/<td\s+bgcolor=['"]#(?:CCCCCC|FFFFFF)['"][^>]*>([\s\S]*?)<\/td>/gi)];
      if (tdMatches.length !== 3) continue;

      const artistsHtml = tdMatches[0][1];
      const venueHtml   = tdMatches[1][1];
      const detailsHtml = tdMatches[2][1];

      // Parse artists
      const artists = artistsHtml
        .split(/<br\s*\/?>/i)
        .map(a => a.replace(/<[^>]+>/g, '').trim())
        .filter(a => a.length > 0);

      if (artists.length === 0) continue;
      if (/^CANCELLED/i.test(artists[0])) continue;

      // Parse venue slug and name
      const venueAnchor = venueHtml.match(/<a\s+href="club\.html#([^"]+)"[^>]*>([^<]+)<\/a>/i);
      const venueSlug = venueAnchor ? venueAnchor[1] : '';
      const venueName = venueAnchor
        ? venueAnchor[2].trim()
        : venueHtml.replace(/<[^>]+>/g, '').trim();

      // Parse details string
      const details = detailsHtml.replace(/<[^>]+>/g, '').trim();
      let age = '', price = '', doors = '', show = '';

      if (/^a\/a/i.test(details)) age = 'all ages';
      else if (/^21\+/.test(details)) age = '21+';
      else if (/^18\+/.test(details)) age = '18+';
      else if (/^5\+/.test(details)) age = '5+';

      const priceMatch = details.match(/\$[\d.]+(?:[/$][\d.]+)*/);
      if (priceMatch) price = priceMatch[0];

      const timeMatch = details.match(/(\d+(?::\d+)?(?:am|pm))(?:\/(\d+(?::\d+)?(?:am|pm)))?/i);
      if (timeMatch) { doors = timeMatch[1]; show = timeMatch[2] || ''; }

      const coords = getVenueCoords(venueSlug);
      const isoDate = currentDate.toISOString().split('T')[0];
      const addrRaw = venueMap[venueSlug] || '';
      // Address is like "1233 17th St., S.F. a/a 21+ 415-xxx" — extract street part
      const addrClean = addrRaw.replace(/\s+(a\/a|21\+|18\+|\d{3}-\d{3}-\d{4}|S\.F\.|Oakland|Berkeley|San Jose).*/i, '').trim();

      events.push({
        id: `thelist-${isoDate}-${venueSlug}-${artists[0].slice(0,20).replace(/\W/g,'_')}`,
        title: artists[0],
        artists,
        venue: venueName,
        venueSlug,
        address: coords?.address || addrClean || '',
        city: coords?.city || (addrRaw.match(/S\.F\./i) ? 'SF' : addrRaw.match(/Oakland/i) ? 'Oakland' : addrRaw.match(/Berkeley/i) ? 'Berkeley' : ''),
        date: isoDate,
        doors,
        show,
        price,
        age,
        category: 'concerts',
        url: 'https://jon.luini.com/thelist/date.html',
        lat: coords?.lat ?? null,
        lng: coords?.lng ?? null,
      });
    }
  } catch (e) {
    console.error('TheList scrape error:', e.message);
  }
  return events;
}

// ─── Generic arts scraper helper ─────────────────────────────────────────────
// Tries multiple selector strategies and deduplicates by title.
function scrapeArtsPage($, selectors, baseUrl, defaults) {
  const seen = new Set();
  const events = [];

  for (const sel of selectors) {
    $(sel).each((_, el) => {
      const title = ($(el).find('h1,h2,h3,h4,.title,[class*="title"]').first().text()
        || $(el).text()).trim().replace(/\s+/g, ' ');
      if (!title || title.length < 4 || title.length > 120) return;
      if (seen.has(title)) return;
      seen.add(title);

      const dateText = $(el).find('time,.date,[class*="date"],[class*="when"]').first().text().trim();
      const rawLink = $(el).find('a').first().attr('href') || '';
      const link = rawLink
        ? (rawLink.startsWith('http') ? rawLink : `${baseUrl}${rawLink}`)
        : `${baseUrl}/`;

      const coords = defaults.coords;
      events.push({
        id: `${defaults.prefix}-${title.slice(0,30).replace(/\W/g,'_')}`,
        title,
        artists: [],
        venue: defaults.venue,
        address: coords.address,
        city: coords.city,
        date: parseDateText(dateText) || '',
        doors: '', show: '', price: '', age: 'all ages',
        category: defaults.category,
        url: link,
        lat: coords.lat,
        lng: coords.lng,
      });
    });
    if (events.length > 0) break; // stop at first strategy that yields results
  }
  return events;
}

// ─── SF Ballet scraper ────────────────────────────────────────────────────────
// Fetches sfballet.org/calendar/ monthly pages and extracts performance data
// from the server-rendered HTML (WordPress + Elementor production_calendar widget).
async function scrapeSFBallet() {
  const coords = ARTS_VENUE_COORDS['SF Ballet'];
  const events = [];
  const seen = new Set();
  const now = new Date();
  const UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36';

  // Month names for URL construction
  const MONTHS = ['January','February','March','April','May','June',
                  'July','August','September','October','November','December'];

  // Fetch current month + next 2 months
  const monthsToFetch = [];
  for (let i = 0; i < 3; i++) {
    const d = new Date(now.getFullYear(), now.getMonth() + i, 1);
    monthsToFetch.push({ year: d.getFullYear(), month: MONTHS[d.getMonth()] });
  }

  // Non-performance items to skip
  const SKIP_TITLES = ['premium', 'voucher', 'gift card', 'opera house tour',
                       'meet the artist', 'dance-along', 'chamber music series'];

  for (const { year, month } of monthsToFetch) {
    try {
      const url = `https://www.sfballet.org/calendar/${year}/${month}/`;
      const html = await fetch(url, { headers: { 'User-Agent': UA } }).then(r => r.text());

      // Split by event-card boundaries
      const blocks = html.split('event-card grid');
      for (const block of blocks.slice(1)) {
        // Mobile date format: "Month Day - Time"
        const dateMatch = block.match(/block lg:hidden[^>]*>\s*(\w+ \d+)\s*-\s*(\d+:\d+\s*[AP]M)/);
        if (!dateMatch) continue;

        // Production link + title
        const linkMatch = block.match(/<a href="((?:https:\/\/www\.sfballet\.org)?\/productions\/[^"]+)">([^<]+)<\/a>/);
        if (!linkMatch) continue;

        const rawDate = dateMatch[1]; // e.g. "April 10"
        const rawTime = dateMatch[2]; // e.g. "8:00 PM"
        const title = linkMatch[2].replace(/&amp;/g, '&').replace(/&#039;/g, "'").trim();
        let eventUrl = linkMatch[1];
        if (eventUrl.startsWith('/')) eventUrl = 'https://www.sfballet.org' + eventUrl;

        // Skip non-performance items
        if (SKIP_TITLES.some(s => title.toLowerCase().includes(s))) continue;

        // Parse date: "April 10" → "2026-04-10"
        const parsed = new Date(`${rawDate}, ${year}`);
        if (isNaN(parsed)) continue;
        // If parsed date is in the past month range, it might belong to previous year context — skip
        if (parsed < new Date(now.getFullYear(), now.getMonth(), 1)) continue;

        const isoDate = parsed.toISOString().split('T')[0];
        const showTime = rawTime.replace(/\s+/g, '').toLowerCase();

        // Deduplicate by date+time+title
        const dedupKey = `${isoDate}-${showTime}-${title}`;
        if (seen.has(dedupKey)) continue;
        seen.add(dedupKey);

        events.push({
          id: `sfballet-${isoDate}-${showTime}-${title.slice(0,20).replace(/\W/g,'_')}`,
          title,
          artists: [],
          venue: 'War Memorial Opera House',
          address: coords.address,
          city: coords.city,
          date: isoDate,
          doors: '', show: showTime, price: 'from $35', age: 'all ages',
          category: 'performing_arts',
          url: eventUrl,
          lat: coords.lat,
          lng: coords.lng,
        });
      }
    } catch (err) {
      console.error(`SF Ballet calendar fetch failed for ${month} ${year}:`, err.message);
    }
  }
  return events;
}

// ─── SF Symphony scraper ──────────────────────────────────────────────────────
// Uses puppeteer-core to bypass Queue-it protection on sfsymphony.org.
// Navigates the calendar month-by-month, extracting events from the DOM.
function findChromePath() {
  const candidates = [
    process.env.CHROME_PATH,
    '/usr/bin/google-chrome',
    '/usr/bin/google-chrome-stable',
    '/usr/bin/chromium-browser',
    '/usr/bin/chromium',
    '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
  ].filter(Boolean);
  for (const p of candidates) {
    try { if (existsSync(p)) return p; } catch {}
  }
  // Try 'which' as last resort
  try { return execSync('which google-chrome || which chromium-browser || which chromium', { encoding: 'utf-8' }).trim(); } catch {}
  return null;
}

async function scrapeSFSymphony() {
  const chromePath = findChromePath();
  if (!chromePath) {
    console.warn('SF Symphony: Chrome not found, skipping scraper');
    return [];
  }

  let puppeteer;
  try {
    puppeteer = await import('puppeteer-core');
  } catch {
    console.warn('SF Symphony: puppeteer-core not available, skipping');
    return [];
  }

  const events = [];
  const seen = new Set();
  const coords = { lat: 37.7774, lng: -122.4196, address: '201 Van Ness Ave', city: 'SF' };
  let browser;

  try {
    browser = await puppeteer.default.launch({
      executablePath: chromePath,
      headless: 'new',
      args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'],
    });
    const page = await browser.newPage();
    await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36');
    await page.goto('https://www.sfsymphony.org/Calendar', { waitUntil: 'networkidle2', timeout: 45000 });

    // Scrape current month + next 2 months
    for (let m = 0; m < 3; m++) {
      const monthData = await page.evaluate(() => {
        const monthYear = document.querySelector('.ui-datepicker-title')?.textContent?.trim() || '';
        const results = [];
        document.querySelectorAll('.calendarDay.currentMonth').forEach(day => {
          const dayNum = day.querySelector('.day-num')?.textContent?.trim();
          day.querySelectorAll('.perfContainer').forEach(card => {
            const titleEl = card.querySelector('.title-container a, h3');
            const rawTitle = titleEl?.textContent?.trim() || '';
            const venue = card.getAttribute('data-venue') || 'Davies Symphony Hall';
            const link = card.querySelector('a.contentlink')?.href || '';
            results.push({ dayNum, rawTitle, venue, link });
          });
        });
        return { monthYear, results };
      });

      // Parse month/year from header (e.g. "April 2026")
      const [monthName, yearStr] = (monthData.monthYear || '').split(/\s+/);
      const year = parseInt(yearStr) || new Date().getFullYear();
      const monthIdx = new Date(`${monthName} 1, 2000`).getMonth();

      for (const { dayNum, rawTitle, venue, link } of monthData.results) {
        // Title has time appended e.g. "Beethoven's Fifth7:30 pm"
        const timeMatch = rawTitle.match(/(\d{1,2}:\d{2}\s*[ap]m)\s*$/i);
        const show = timeMatch ? timeMatch[1].replace(/\s+/g, '').toLowerCase() : '';
        const title = rawTitle.replace(/\d{1,2}:\d{2}\s*[ap]m\s*$/i, '').trim();
        if (!title || !dayNum) continue;

        const day = parseInt(dayNum);
        const dateObj = new Date(year, monthIdx, day);
        if (isNaN(dateObj.getTime())) continue;
        const isoDate = dateObj.toISOString().split('T')[0];

        // Skip past events
        if (dateObj < new Date(new Date().toDateString())) continue;

        const dedupKey = `${isoDate}-${show}-${title}`;
        if (seen.has(dedupKey)) continue;
        seen.add(dedupKey);

        // Resolve venue to known coordinates
        const venueName = (venue === 'undefined' || !venue) ? 'Davies Symphony Hall' : venue;

        events.push({
          id: `sfsymphony-${isoDate}-${show}-${title.slice(0,20).replace(/\W/g,'_')}`,
          title,
          artists: [],
          venue: venueName,
          address: coords.address,
          city: coords.city,
          date: isoDate,
          doors: '', show, price: '', age: 'all ages',
          category: 'performing_arts',
          url: link || 'https://www.sfsymphony.org/Calendar',
          lat: coords.lat,
          lng: coords.lng,
        });
      }

      // Navigate to next month (if not last iteration)
      if (m < 2) {
        try {
          await page.click('button.calendar__next');
          await new Promise(r => setTimeout(r, 3000));
        } catch {
          break; // no more months
        }
      }
    }
  } catch (err) {
    console.error('SF Symphony scraper error:', err.message);
  } finally {
    if (browser) await browser.close().catch(() => {});
  }
  return events;
}

// ─── BroadwaySF scraper ───────────────────────────────────────────────────────
// Scrapes shows from broadwaysf.com (ATG/Ambassador Theatre Group) which covers
// Orpheum Theatre, Golden Gate Theatre, and Curran Theatre.
// Uses ATG's public bolt API for show info + GraphQL calendar API for performance dates.
const BSF_VENUE_MAP = {
  'orpheum-theatre':      'Orpheum Theatre',
  'golden-gate-theatre':  'Golden Gate Theatre',
  'curran-theater':       'Curran Theatre',
};

async function scrapeBroadwaySF() {
  const events = [];
  const UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36';

  const CALENDAR_GQL = 'https://calendar-service.core.platform.atgtickets.com/';
  const BOLT_API = 'https://boltapi-us-west.atgtickets.com/shows';
  const GQL_QUERY = `query getShow($titleSlug: String, $venueSlug: String, $combined: Boolean, $ruleSetting: RuleSetting, $sourceId: String) {
    getShow(titleSlug: $titleSlug, venueSlug: $venueSlug, combined: $combined, ruleSetting: $ruleSetting, sourceId: $sourceId) {
      show { status dates { nextPerformanceDate lastPerformanceDate timeZone } performances { id dates { performanceDate } status } }
    }
  }`;

  try {
    // Step 1: discover show slugs from the homepage
    const homeHtml = await fetch('https://www.broadwaysf.com/', { headers: { 'User-Agent': UA } }).then(r => r.text());
    const linkMatches = [...homeHtml.matchAll(/\/events\/([^/]+)\/(orpheum-theatre|golden-gate-theatre|curran-theater)(?:\/|")/g)];
    const showMap = new Map();
    for (const m of linkMatches) {
      const key = `${m[1]}/${m[2]}`;
      if (!showMap.has(key)) showMap.set(key, { titleSlug: m[1], venueSlug: m[2] });
    }

    // Step 2: for each show, fetch title from bolt API + performances from calendar GraphQL
    for (const [key, { titleSlug, venueSlug }] of showMap) {
      try {
        // Fetch show info (title, price, venue details)
        const showInfo = await fetch(`${BOLT_API}/${titleSlug}/${venueSlug}`, {
          headers: { 'Content-Type': 'application/json', 'User-Agent': UA }
        }).then(r => r.json()).catch(() => null);

        const title = showInfo?.title || titleSlug.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
        const venueName = BSF_VENUE_MAP[venueSlug] || showInfo?.venueInfo?.name || venueSlug;
        const coords = ARTS_VENUE_COORDS[venueName] || {};
        const priceMin = showInfo?.priceInfo?.min;
        const price = priceMin ? `from $${priceMin}` : '';

        // Fetch performances from GraphQL calendar
        const calRes = await fetch(CALENDAR_GQL, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'Origin': 'https://www.broadwaysf.com' },
          body: JSON.stringify({
            operationName: 'getShow',
            variables: { titleSlug, venueSlug, combined: false, ruleSetting: {}, sourceId: 'AV_US_WEST' },
            query: GQL_QUERY,
          }),
        }).then(r => r.json()).catch(() => null);

        const show = calRes?.data?.getShow?.show;
        if (!show || !show.performances?.length) continue;

        const tz = show.dates?.timeZone || 'America/Los_Angeles';

        for (const perf of show.performances) {
          if (perf.status !== 'OKAY') continue;
          const utcDate = perf.dates?.performanceDate;
          if (!utcDate) continue;

          // Convert UTC to local date/time
          const dt = new Date(utcDate);
          const local = new Date(dt.toLocaleString('en-US', { timeZone: tz }));
          const isoDate = `${local.getFullYear()}-${String(local.getMonth()+1).padStart(2,'0')}-${String(local.getDate()).padStart(2,'0')}`;
          const hours = local.getHours();
          const mins = local.getMinutes();
          const ampm = hours >= 12 ? 'pm' : 'am';
          const h12 = hours % 12 || 12;
          const showTime = `${h12}:${String(mins).padStart(2,'0')}${ampm}`;

          // Skip past events
          if (dt < new Date()) continue;

          const eventUrl = `https://www.broadwaysf.com/events/${titleSlug}/${venueSlug}/`;

          events.push({
            id: `bsf-${perf.id}`,
            title,
            artists: [],
            venue: venueName,
            address: coords.address || '',
            city: coords.city || 'SF',
            date: isoDate,
            doors: '', show: showTime, price, age: 'all ages',
            category: 'performing_arts',
            url: eventUrl,
            lat: coords.lat || 0,
            lng: coords.lng || 0,
          });
        }
      } catch (err) {
        console.error(`BroadwaySF: failed for ${key}:`, err.message);
      }
    }
  } catch (err) {
    console.error('BroadwaySF scraper error:', err.message);
  }
  return events;
}

// ─── SF Opera scraper ─────────────────────────────────────────────────────────
// Fetches buy-tickets page → extracts opera slugs → fetches each production
// page for actual performance dates (up to 8 productions, limited to avoid rate limits)
async function scrapeSFOpera() {
  const events = [];
  const coords = ARTS_VENUE_COORDS['SF Opera'];
  try {
    const listHtml = await fetch('https://www.sfopera.com/buy-tickets/', {
      headers: { 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36' }
    }).then(r => r.text());

    // Extract opera slugs: /operas/SLUG/#performances and /seasons/SLUG/#performances
    const slugMatches = [...listHtml.matchAll(/href="(\/(?:operas|seasons)\/([^/"]+))\/#performances"/g)];
    const slugs = [...new Map(slugMatches.map(m => [m[2], m[1]])).entries()].slice(0, 10);

    for (const [slug, path] of slugs) {
      try {
        const prodHtml = await fetch(`https://www.sfopera.com${path}/`, {
          headers: { 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36' }
        }).then(r => r.text());

        // Extract title from <h1>
        const titleMatch = prodHtml.match(/<h1[^>]*>([\s\S]{0,200}?)<\/h1>/i);
        const title = titleMatch
          ? titleMatch[1].replace(/<[^>]+>/g,'').trim()
          : slug.replace(/-/g,' ').replace(/\b\w/g, c => c.toUpperCase());

        // Extract performance dates
        const dateMatches = [...prodHtml.matchAll(/(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+\d{1,2}(?:,\s*\d{4})?/gi)];
        const timeMatches = [...prodHtml.matchAll(/\d{1,2}:\d{2}\s*(?:AM|PM)/gi)];
        const show = timeMatches[0]?.[0]?.replace(/\s+/,'').toLowerCase() || '7:30pm';

        // Deduplicate dates
        const seenDates = new Set();
        for (const dm of dateMatches) {
          const rawDate = dm[0];
          const parsed = parseDateText(rawDate + (rawDate.includes(',') ? '' : ', 2026'));
          if (!parsed || seenDates.has(parsed)) continue;
          // Skip past dates
          if (new Date(parsed + 'T00:00:00') < new Date(Date.now() - 86400000)) continue;
          seenDates.add(parsed);
          events.push({
            id: `sfopera-${parsed}-${slug.slice(0,20)}`,
            title,
            artists: [],
            venue: 'War Memorial Opera House',
            address: coords.address,
            city: coords.city,
            date: parsed,
            doors: '', show,
            price: 'from $28', age: 'all ages',
            category: 'performing_arts',
            url: `https://www.sfopera.com${path}/`,
            lat: coords.lat,
            lng: coords.lng,
          });
        }
      } catch (e) { /* skip failed production */ }
    }
  } catch (e) {
    console.error('SF Opera scrape error:', e.message);
  }
  return events;
}

// ─── ACT Theater scraper ──────────────────────────────────────────────────────
// act-sf.org is JS-rendered. Static fallback for 2025-26 season.
async function scrapeACT() {
  const coords = ARTS_VENUE_COORDS['ACT'];
  const productions = [
    { title: 'Appropriate', dates: ['2026-04-08','2026-04-10','2026-04-12','2026-04-15','2026-04-17','2026-04-19'], url: 'https://www.act-sf.org/productions' },
    { title: 'Mlima\'s Tale', dates: ['2026-05-06','2026-05-08','2026-05-10','2026-05-13','2026-05-15'], url: 'https://www.act-sf.org/productions' },
    { title: 'The Sign in Sidney Brustein\'s Window', dates: ['2026-06-03','2026-06-05','2026-06-07','2026-06-10'], url: 'https://www.act-sf.org/productions' },
  ];
  const events = [];
  for (const prod of productions) {
    for (const date of prod.dates) {
      if (new Date(date + 'T00:00:00') < new Date(Date.now() - 86400000)) continue;
      events.push({
        id: `act-${date}-${prod.title.slice(0,20).replace(/\W/g,'_')}`,
        title: prod.title,
        artists: [],
        venue: 'ACT Geary Theater',
        address: coords.address,
        city: coords.city,
        date,
        doors: '', show: '7:30pm', price: 'from $25', age: 'all ages',
        category: 'performing_arts',
        url: prod.url,
        lat: coords.lat,
        lng: coords.lng,
      });
    }
  }
  return events;
}

// ─── SFMOMA scraper ───────────────────────────────────────────────────────────
async function scrapeSFMOMA() {
  try {
    // Try fetching SFMOMA events page
    const html = await fetch('https://www.sfmoma.org/events/', {
      headers: { 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36' }
    }).then(r => r.text());
    const $ = cheerio.load(html);
    const coords = ARTS_VENUE_COORDS['SFMOMA'];

    // Look for JSON-LD events
    const events = [];
    $('script[type="application/ld+json"]').each((_, el) => {
      try {
        const data = JSON.parse($(el).html());
        const items = Array.isArray(data) ? data : (data['@graph'] || [data]);
        for (const item of items) {
          if (item['@type'] !== 'Event') continue;
          events.push({
            id: `sfmoma-${(item.name||'').slice(0,30).replace(/\W/g,'_')}`,
            title: item.name || 'SFMOMA Event',
            artists: [],
            venue: 'SFMOMA',
            address: coords.address,
            city: coords.city,
            date: parseDateText(item.startDate) || '',
            doors: '', show: '', price: item.offers?.price ? `$${item.offers.price}` : '', age: 'all ages',
            category: 'museum',
            url: item.url || 'https://www.sfmoma.org/events/',
            lat: coords.lat,
            lng: coords.lng,
          });
        }
      } catch (_) {}
    });

    if (events.length === 0) {
      // Fallback: generic entry for visiting SFMOMA
      events.push({
        id: 'sfmoma-ongoing',
        title: 'SFMOMA – Current Exhibitions',
        artists: [],
        venue: 'SFMOMA',
        address: coords.address,
        city: coords.city,
        date: new Date().toISOString().split('T')[0],
        doors: '10am', show: '', price: 'from $25', age: 'all ages',
        category: 'museum',
        url: 'https://www.sfmoma.org/events/',
        lat: coords.lat,
        lng: coords.lng,
      });
    }
    return events;
  } catch (e) {
    console.error('SFMOMA scrape error:', e.message);
    return [];
  }
}

// ─── Oakland Museum of California (OMCA) scraper ─────────────────────────────
// Uses JSON-LD Event schema on their events listing page
async function scrapeOaklandMuseum() {
  const coords = ARTS_VENUE_COORDS['Oakland Museum'];
  try {
    const resp = await fetch('https://museumca.org/events/', {
      headers: { 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36' }
    });
    if (!resp.ok) return [];
    const html = await resp.text();
    if (/error\s+40[34]|forbidden|cloudflare|just a moment/i.test(html.slice(0, 2000))) return [];

    const events = [];
    const $ = cheerio.load(html);
    $('script[type="application/ld+json"]').each((_, el) => {
      try {
        const data = JSON.parse($(el).html());
        const items = Array.isArray(data) ? data : (data['@graph'] || [data]);
        for (const item of items) {
          if (item['@type'] !== 'Event') continue;
          const title = (item.name || '').replace(/\\n/g, ' ').replace(/&amp;/g, '&').replace(/&#[\d]+;/g, '').replace(/\s+/g, ' ').trim();
          if (!title) continue;
          const startDate = item.startDate ? item.startDate.split('T')[0] : '';
          const rawPrice = item.offers?.price;
          const price = rawPrice === 0 || rawPrice === '0' || String(rawPrice).toLowerCase() === 'free'
            ? 'Free'
            : rawPrice && String(rawPrice).replace(/[^0-9.]/g, '') !== ''
              ? `$${String(rawPrice).replace(/[^0-9.\-–]/g, '')}`
              : rawPrice ? String(rawPrice) : '';
          events.push({
            id: `omca-${startDate}-${title.slice(0,25).replace(/\W/g,'_')}`,
            title,
            artists: [],
            venue: 'Oakland Museum of California',
            address: coords.address,
            city: coords.city,
            date: startDate,
            doors: '', show: item.startDate?.split('T')[1]?.slice(0,5) || '',
            price, age: 'all ages',
            category: 'museum',
            url: item.url || 'https://museumca.org/events/',
            lat: coords.lat,
            lng: coords.lng,
          });
        }
      } catch (_) {}
    });
    return events;
  } catch (e) {
    console.error('Oakland Museum scrape error:', e.message);
    return [];
  }
}

// ─── YBCA scraper ─────────────────────────────────────────────────────────────
// Parses .feature-event-wrap cards: .type, h3 a (title), .date, .tickets p
async function scrapeYBCA() {
  const coords = ARTS_VENUE_COORDS['YBCA'];
  try {
    const resp = await fetch('https://www.ybca.org/calendar/', {
      headers: { 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36' }
    });
    if (!resp.ok) return [];
    const html = await resp.text();
    if (/error\s+40[34]|forbidden|cloudflare|just a moment/i.test(html.slice(0, 2000))) return [];

    const events = [];
    const $ = cheerio.load(html);
    const seen = new Set();

    $('.feature-event-wrap').each((_, card) => {
      const $card = $(card);
      const title = $card.find('h3 a, h2 a').first().text().trim();
      if (!title || seen.has(title)) return;
      seen.add(title);

      const url = $card.find('h3 a, h2 a').first().attr('href') || 'https://www.ybca.org/calendar/';
      const dateRaw = $card.find('.date').first().text().trim();
      // Format: "Thursday, April 2, 2026, 7 PM" or "April 11–19, 2026"
      const dateMatch = dateRaw.match(/(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+(\d{1,2})(?:[-–]\d{1,2})?,?\s*(\d{4})/i);
      const date = dateMatch ? parseDateText(`${dateMatch[1]} ${dateMatch[2]}, ${dateMatch[3]}`) : '';

      // Time from date string
      const timeMatch = dateRaw.match(/,\s*(\d{1,2}(?::\d{2})?\s*(?:AM|PM))/i);
      const show = timeMatch ? timeMatch[1].toLowerCase().replace(' ', '') : '';

      // Price from .tickets paragraph
      const ticketText = $card.find('.tickets').text();
      const priceMatch = ticketText.match(/\$[\d,]+(?:[-–]\$?[\d,]+)?/);
      const price = priceMatch ? priceMatch[0] : (ticketText.toLowerCase().includes('free') ? 'Free' : '');

      // Venue: the card sometimes specifies a room/venue
      const venueMatch = ticketText.match(/(?:The\s+)?([A-Z][^,.]{3,40}),\s*YBCA/);
      const venue = venueMatch ? `${venueMatch[1]}, YBCA` : 'YBCA';

      // Skip exhibitions with no specific date (ongoing shows)
      if (!date) return;

      events.push({
        id: `ybca-${date}-${title.slice(0,25).replace(/\W/g,'_')}`,
        title,
        artists: [],
        venue,
        address: coords.address,
        city: coords.city,
        date,
        doors: '', show,
        price, age: 'all ages',
        category: 'museum',
        url: url.startsWith('http') ? url : `https://www.ybca.org${url}`,
        lat: coords.lat,
        lng: coords.lng,
      });
    });
    return events;
  } catch (e) {
    console.error('YBCA scrape error:', e.message);
    return [];
  }
}

// ─── Asian Art Museum scraper ─────────────────────────────────────────────────
// Fetches calendar.asianart.org, extracts event links + the few server-rendered
// <time> tags; for links with dates in the slug, parses those too.
async function scrapeAsianArtMuseum() {
  const coords = ARTS_VENUE_COORDS['Asian Art Museum'];
  try {
    const resp = await fetch('https://calendar.asianart.org/', {
      headers: { 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36' }
    });
    if (!resp.ok) return [];
    const html = await resp.text();
    if (/error\s+40[34]|forbidden|cloudflare|just a moment/i.test(html.slice(0, 2000))) return [];

    const $ = cheerio.load(html);
    const events = [];
    const seen = new Set();
    const today = new Date(); today.setHours(0,0,0,0);
    const currentYear = today.getFullYear();

    // The listing page renders only a few events server-side with <time> tags.
    // Each event block has: <time>DAY, MONTH D / TIME</time> and a nearby <a> to the event
    $('time').each((_, el) => {
      const $el = $(el);
      const timeText = $el.text().trim(); // e.g. "THU, APRIL 2 / 7:00PM - 9:00PM"
      // Find the closest parent that also has an <a> with event URL
      const $parent = $el.closest('[class]');
      const url = $parent.find('a[href*="/event/"]').first().attr('href')
        || $el.nextAll('a[href*="/event/"]').first().attr('href')
        || $el.closest('li,div,article').find('a[href*="/event/"]').first().attr('href') || '';
      const rawTitle = $parent.find('h2,h3,h4,.event-title,[class*="title"]').first().text().trim();
      const slugTitle = url
        ? decodeURIComponent(url.split('/event/')[1]?.replace(/\/$/,'') || '')
            .replace(/-+/g, ' ').trim().replace(/\b\w/g, c => c.toUpperCase())
        : '';
      const title = rawTitle || slugTitle;

      // Parse date from time text: "THU, APRIL 2 / 7:00PM"
      const dtMatch = timeText.match(/(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+(\d{1,2})/i);
      if (!dtMatch) return;
      const date = parseDateText(`${dtMatch[1]} ${dtMatch[2]}, ${currentYear}`);
      if (!date || seen.has(url || title)) return;
      seen.add(url || title);

      const showMatch = timeText.match(/(\d{1,2}:\d{2}(?:AM|PM))/i);
      const show = showMatch ? showMatch[1].toLowerCase() : '';

      events.push({
        id: `aam-${date}-${(title || url).slice(0,25).replace(/\W/g,'_')}`,
        title: title || 'Asian Art Museum Event',
        artists: [],
        venue: 'Asian Art Museum',
        address: coords.address,
        city: coords.city,
        date,
        doors: '', show,
        price: '', age: 'all ages',
        category: 'museum',
        url: url || 'https://calendar.asianart.org/',
        lat: coords.lat,
        lng: coords.lng,
      });
    });

    // Also extract recurring/upcoming events from slugs that embed a date
    // Pattern: /event/some-title-april-5/ or /event/some-thing-apr-8-2026/
    const MONTHS_SHORT = { jan:0,feb:1,mar:2,apr:3,may:4,jun:5,jul:6,aug:7,sep:8,oct:9,nov:10,dec:11,
      january:0,february:1,march:2,april:3,june:5,july:6,august:7,september:8,october:9,november:10,december:11 };
    const linkRe = /href="(https:\/\/calendar\.asianart\.org\/event\/([^"]+))"/g;
    let lm;
    while ((lm = linkRe.exec(html)) !== null) {
      const url = lm[1];
      const slug = lm[2].replace(/\/$/, '');
      if (seen.has(url)) continue;

      // Try to extract date from slug: "...-april-5" or "...-apr-8-2026"
      const slugDateMatch = slug.match(/-(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)-(\d{1,2})(?:-(\d{4}))?(?:\/|$|-[a-z])/i);
      if (!slugDateMatch) continue;

      const monthIdx = MONTHS_SHORT[slugDateMatch[1].toLowerCase()];
      const day = parseInt(slugDateMatch[2]);
      const year = parseInt(slugDateMatch[3] || currentYear);
      const dt = new Date(year, monthIdx, day);
      if (isNaN(dt.getTime()) || dt < today) continue;

      const date = dt.toISOString().split('T')[0];
      seen.add(url);

      // Derive readable title from slug: strip date suffix, convert hyphens to spaces, title-case
      const titleFromSlug = slug
        .replace(new RegExp(`-?${slugDateMatch[1]}-${slugDateMatch[2]}[^/]*$`, 'i'), '')
        .replace(/-+/g, ' ')
        .trim()
        .replace(/\b\w/g, c => c.toUpperCase());

      events.push({
        id: `aam-${date}-${slug.slice(0,25).replace(/\W/g,'_')}`,
        title: titleFromSlug || 'Asian Art Museum Event',
        artists: [],
        venue: 'Asian Art Museum',
        address: coords.address,
        city: coords.city,
        date,
        doors: '', show: '',
        price: '', age: 'all ages',
        category: 'museum',
        url,
        lat: coords.lat,
        lng: coords.lng,
      });
    }

    return events;
  } catch (e) {
    console.error('Asian Art Museum scrape error:', e.message);
    return [];
  }
}

// ─── De Young / FAMSF scraper ─────────────────────────────────────────────────
// The FAMSF site is a JS-rendered React app with no server-side event HTML.
// We generate placeholder entries pointing to the events page.
async function scrapeDeYoung() {
  const coords = ARTS_VENUE_COORDS['de Young'];
  const today = new Date().toISOString().split('T')[0];
  return [
    {
      id: 'deyoung-exhibitions',
      title: 'de Young Museum – Current Exhibitions & Events',
      artists: [],
      venue: 'de Young Museum',
      address: coords.address,
      city: coords.city,
      date: today,
      doors: '9:30am', show: '', price: 'from $15', age: 'all ages',
      category: 'museum',
      url: 'https://www.famsf.org/events',
      lat: coords.lat,
      lng: coords.lng,
    },
  ];
}

// ─── Stanford Theater scraper ────────────────────────────────────────────────
// Parses the homepage (current week + next program links) then fetches each
// linked calendar page to extract the full per-weekend schedule.
async function scrapeStanfordTheater() {
  const BASE = 'https://www.stanfordtheatre.org';
  const coords = ARTS_VENUE_COORDS['Stanford Theater'];
  const events = [];
  const today = new Date(); today.setHours(0,0,0,0);

  try {
    // 1. Fetch homepage to find active calendar links
    const homeHtml = await fetch(BASE, {
      headers: { 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36' }
    }).then(r => r.text());

    // Collect calendar hrefs: both "calendars/Foo.html" links and "aboutWeek.html"
    const calLinks = new Set();
    const linkRe = /href=["'](calendars\/[^"'?#]+\.html|aboutWeek\.html)["']/gi;
    let m;
    while ((m = linkRe.exec(homeHtml)) !== null) calLinks.add(m[1]);

    // Parse each calendar page
    for (const rel of calLinks) {
      try {
        const url = `${BASE}/${rel.split('/').map(encodeURIComponent).join('/')}`;
        const html = await fetch(url, {
          headers: { 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36' }
        }).then(r => r.text());

        events.push(...parseStanfordCalendarPage(html, url, coords, today));
      } catch (e) { /* skip failed page */ }
    }

    // 2. Also parse aboutWeek.html (current weekend) if not already included
    if (!calLinks.has('aboutWeek.html')) {
      try {
        const html = await fetch(`${BASE}/aboutWeek.html`, {
          headers: { 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36' }
        }).then(r => r.text());
        events.push(...parseStanfordWeekPage(html, `${BASE}/aboutWeek.html`, coords, today));
      } catch (e) { /* skip */ }
    }
  } catch (e) {
    console.error('Stanford Theater scrape error:', e.message);
  }

  // Deduplicate by id
  const seen = new Set();
  return events.filter(e => { if (seen.has(e.id)) return false; seen.add(e.id); return true; });
}

// Parse a structured calendar page like "Hitchcock 2026.html"
// Each <td class="playdate"> contains a <p class="date">Month D-D</p>
// followed by <p><a href="...">Film Title (year)</a> times</p> entries
function parseStanfordCalendarPage(html, url, coords, today) {
  const events = [];
  const $ = cheerio.load(html);
  const currentYear = new Date().getFullYear();

  const MONTHS = { january:0,february:1,march:2,april:3,may:4,june:5,
    july:6,august:7,september:8,october:9,november:10,december:11,
    jan:0,feb:1,mar:2,apr:3,jun:5,jul:6,aug:7,sep:8,oct:9,nov:10,dec:11 };

  // Each .playdate td contains a date range + one or more films with showtimes
  $('td.playdate').each((_, td) => {
    const $td = $(td);

    // Extract date range from <p class="date">
    const dateText = $td.find('p.date, .date').first().text().trim();
    const rangeMatch = dateText.match(/(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+(\d{1,2})[-–](\d{1,2})/i);
    if (!rangeMatch) return;

    const monthKey = rangeMatch[1].toLowerCase().slice(0, 3);
    const monthIdx = MONTHS[monthKey] ?? MONTHS[rangeMatch[1].toLowerCase()];
    const startDay = parseInt(rangeMatch[2]);
    const endDay   = parseInt(rangeMatch[3]);

    const weekendDates = [];
    for (let d = startDay; d <= endDay; d++) {
      const dt = new Date(currentYear, monthIdx, d);
      if (dt >= today) weekendDates.push(dt);
    }
    if (weekendDates.length === 0) return;

    // Each film is in a <p> with an <a> link; showtimes follow the film title
    $td.find('p').each((_, p) => {
      const $p = $(p);
      const pText = $p.text().replace(/\s+/g, ' ').trim();

      // Film: has a year in parentheses
      const filmMatch = pText.match(/^(.{4,80}?\(\d{4}\))\s*(.*)$/);
      if (!filmMatch) return;

      const filmTitle = filmMatch[1].trim();
      const timePart  = filmMatch[2].trim(); // rest of line: "7:30 (plus 3:25 Sat/Sun)"

      // Parse all showtimes mentioned
      const timeRe = /(\d{1,2}:\d{2})/g;
      let tm;
      const times = [];
      while ((tm = timeRe.exec(timePart)) !== null) times.push(tm[1]);
      const showtime = times[0] ? times[0] + 'pm' : '7:30pm';

      // Check if specific days are mentioned (e.g. "Sat/Sun", "plus 3:25 Sat/Sun")
      const satSunOnly = /\b(sat|sun)/i.test(timePart) && !/\b(fri|thu)/i.test(timePart);
      const thuFriOnly = /\b(thu|fri)/i.test(timePart) && !/\b(sat|sun)/i.test(timePart);

      for (const dt of weekendDates) {
        const dayOfWeek = dt.getDay(); // 0=Sun,4=Thu,5=Fri,6=Sat
        if (satSunOnly && dayOfWeek !== 6 && dayOfWeek !== 0) continue;
        if (thuFriOnly && dayOfWeek !== 4 && dayOfWeek !== 5) continue;

        const iso = dt.toISOString().split('T')[0];
        events.push({
          id: `stanford-${iso}-${filmTitle.slice(0,25).replace(/\W/g,'_')}`,
          title: filmTitle,
          artists: [],
          venue: 'Stanford Theater',
          address: coords.address,
          city: coords.city,
          date: iso,
          doors: '', show: showtime,
          price: '$10', age: 'all ages',
          category: 'film',
          url,
          lat: coords.lat,
          lng: coords.lng,
        });
      }
    });
  });

  // Fallback: if the structured parse yielded nothing, try the simpler weekly format
  if (events.length === 0) {
    return parseStanfordWeekPage(html, url, coords, today);
  }
  return events;
}

// Parse the simpler "aboutWeek.html" page: one film, explicit day/time sentences
function parseStanfordWeekPage(html, url, coords, today) {
  const events = [];
  const $ = cheerio.load(html);
  const currentYear = new Date().getFullYear();

  // Title is in a colored <span> inside <p>
  const titleEl = $('span[style*="color"]').filter((_, el) => {
    const t = $(el).text().trim();
    return t.length > 4 && /\(\d{4}\)/.test(t);
  }).first();
  const title = titleEl.text().trim();
  if (!title) return events;

  // Parse showtime sentence: "Shows at 7:30 Friday, 2:00 and 7:30 Saturday, and 2:00 Sunday."
  const bodyText = $('body').text();
  const showsMatch = bodyText.match(/Shows?\s+at\s+(.{10,120}?)\.?\s*$/im);
  if (!showsMatch) return events;

  const showsText = showsMatch[1];
  const DAY_MAP = { friday: 5, saturday: 6, sunday: 0, thursday: 4 };

  // Find the nearest upcoming Friday for this current weekend
  const ref = new Date(today);
  // Walk forward to find the next Fri/Sat/Sun block
  while (ref.getDay() !== 5) ref.setDate(ref.getDate() + 1); // advance to Friday

  // Parse each "N:NN [DayName]" occurrence
  const timeRe = /(\d{1,2}:\d{2})\s+(\w+day)/gi;
  let m;
  while ((m = timeRe.exec(showsText)) !== null) {
    const show = m[1];
    const dayName = m[2].toLowerCase();
    const targetDow = DAY_MAP[dayName];
    if (targetDow === undefined) continue;

    const dt = new Date(ref);
    const diff = (targetDow - ref.getDay() + 7) % 7;
    dt.setDate(ref.getDate() + diff);
    if (dt < today) continue;

    const iso = dt.toISOString().split('T')[0];
    events.push({
      id: `stanford-${iso}-${title.slice(0,25).replace(/\W/g,'_')}-${show.replace(':','')}`,
      title,
      artists: [],
      venue: 'Stanford Theater',
      address: coords.address,
      city: coords.city,
      date: iso,
      doors: '', show: show + (parseInt(show) < 12 ? 'pm' : 'am'),
      price: '$10', age: 'all ages',
      category: 'film',
      url,
      lat: coords.lat,
      lng: coords.lng,
    });
  }
  return events;
}

// ─── BAMPFA scraper ───────────────────────────────────────────────────────────
// bampfa.org blocks direct calendar scraping (403). Fetch their ticketing page
// which lists upcoming programs with titles and dates in static HTML.
async function scrapeBAMPFA() {
  const coords = ARTS_VENUE_COORDS['BAMPFA'];
  try {
    // The calendar page at /visit/calendar returns server-rendered HTML with real event data.
    // Each event is in a .views-row containing a .calendar-event (brief title) and a
    // .popupboxthing (full detail: .popupboxthing-date, .popupboxthing-time, .title a).
    // Note: BAMPFA blocks browser User-Agent strings — fetch with Node's default headers
    const resp = await fetch('https://bampfa.org/visit/calendar');
    if (!resp.ok) return [];
    const html = await resp.text();
    if (/error\s+40[34]|forbidden|access denied|cloudflare|just a moment/i.test(html.slice(0, 2000))) return [];

    const $ = cheerio.load(html);
    const events = [];
    const seen = new Set();
    const today = new Date(); today.setHours(0,0,0,0);

    // Each .views-row has a .calendar-event (compact) + a .popupboxthing (detail with date)
    $('.views-row').each((_, row) => {
      const $row = $(row);

      // Title from the popup detail (more reliable than the compact grid cell)
      const $popup = $row.find('.popupboxthing').first();
      if (!$popup.length) return;

      const title = $popup.find('.title a, .event-content .title').first().text().trim()
        || $popup.find('h2,h3,h4').first().text().trim();
      if (!title || seen.has(title)) return;

      const dateRaw = $popup.find('.popupboxthing-date').first().text().trim();
      // Format: "Sunday, March 29, 2026"
      const date = parseDateText(dateRaw);
      if (!date) return;

      // Skip past events
      const dt = new Date(date + 'T00:00:00');
      if (dt < today) return;

      seen.add(title);

      const timeRaw = $popup.find('.popupboxthing-time').first().text().trim(); // "11 AM–7 PM"
      const showMatch = timeRaw.match(/(\d{1,2}(?::\d{2})?\s*(?:AM|PM))/i);
      const show = showMatch ? showMatch[1].toLowerCase().replace(' ', '') : '';

      const relUrl = $popup.find('.title a').first().attr('href') || '';
      const url = relUrl.startsWith('http') ? relUrl : `https://bampfa.org${relUrl}`;

      // Determine category: film events vs art/gallery events
      const filterTag = $row.find('.calendar_filter li').first().text().trim().toLowerCase();
      const category = filterTag === 'film' ? 'film' : 'museum';

      // Price from ticketing_information
      const priceText = $popup.find('.ticketing_information, .admission_information').text().trim();
      const priceMatch = priceText.match(/\$[\d,.]+(?:\s*[-–]\s*\$?[\d,.]+)?/);
      const price = priceMatch ? priceMatch[0] : (priceText.toLowerCase().includes('free') ? 'Free' : '$10–$15');

      events.push({
        id: `bampfa-${date}-${title.slice(0,25).replace(/\W/g,'_')}`,
        title,
        artists: [],
        venue: 'BAMPFA',
        address: coords.address,
        city: coords.city,
        date,
        doors: '', show,
        price, age: 'all ages',
        category,
        url: url || 'https://bampfa.org/visit/calendar',
        lat: coords.lat,
        lng: coords.lng,
      });
    });

    return events.length > 0 ? events : [];
  } catch (e) {
    console.error('BAMPFA scrape error:', e.message);
    return [];
  }
}


// ─── Date text parser (best-effort) ──────────────────────────────────────────
function parseDateText(text) {
  if (!text) return null;
  // Try to find a date pattern
  const patterns = [
    /(\d{4}-\d{2}-\d{2})/,                           // ISO
    /(\w+ \d{1,2},?\s*\d{4})/,                        // "April 5, 2026"
    /(\d{1,2}\/\d{1,2}\/\d{2,4})/,                   // 4/5/26
    /(\w+ \d{1,2}(?:\s*[-–]\s*\w* \d{1,2})?)/,       // "April 5" or "April 5 - May 2"
  ];
  for (const pat of patterns) {
    const m = text.match(pat);
    if (m) {
      try {
        const d = new Date(m[1]);
        if (!isNaN(d.getTime())) return d.toISOString().split('T')[0];
      } catch (_) {}
    }
  }
  return null;
}

// ─── Geocode events missing coords ───────────────────────────────────────────
async function enrichCoords(events) {
  const promises = events.map(async (ev) => {
    if (ev.lat && ev.lng) return ev;
    const coords = await geocodeVenue(ev.venue, ev.city || 'San Francisco');
    if (coords) {
      ev.lat = coords.lat;
      ev.lng = coords.lng;
    }
    return ev;
  });
  return Promise.all(promises);
}

// ─── Sources registry ─────────────────────────────────────────────────────────
const SOURCES = [
  { key: 'thelist',  label: 'TheList Concerts',  fn: scrapeTheList },
  { key: 'sfopera',  label: 'SF Opera',           fn: scrapeSFOpera },
  { key: 'sfballet', label: 'SF Ballet',          fn: scrapeSFBallet },
  { key: 'sfsymphony', label: 'SF Symphony',      fn: scrapeSFSymphony },
  { key: 'broadwaysf', label: 'BroadwaySF',       fn: scrapeBroadwaySF },
  { key: 'act',      label: 'ACT Theater',        fn: scrapeACT },
  { key: 'sfmoma',      label: 'SFMOMA',              fn: scrapeSFMOMA },
  { key: 'omca',        label: 'Oakland Museum',     fn: scrapeOaklandMuseum },
  { key: 'ybca',        label: 'YBCA',               fn: scrapeYBCA },
  { key: 'asianart',    label: 'Asian Art Museum',   fn: scrapeAsianArtMuseum },
  { key: 'deyoung',     label: 'de Young',           fn: scrapeDeYoung },
  { key: 'bampfa',      label: 'BAMPFA',             fn: scrapeBAMPFA },
  { key: 'stanford', label: 'Stanford Theater',   fn: scrapeStanfordTheater },
];

// In-flight scrape promise — prevents duplicate concurrent scrapes
let scrapeInFlight = null;

async function fetchAllEvents() {
  const allEvents = [];
  await Promise.allSettled(
    SOURCES.map(async ({ fn }) => {
      const evs = await fn();
      allEvents.push(...evs);
    })
  );
  const missing = allEvents.filter(e => !e.lat || !e.lng).slice(0, 20);
  if (missing.length > 0) await enrichCoords(missing);
  console.log(`Fetched ${allEvents.length} events total`);
  return allEvents;
}

// ─── API routes ───────────────────────────────────────────────────────────────

// Fast endpoint: returns cached JSON or kicks off background scrape + returns partial
app.get('/api/events', async (req, res) => {
  const cached = cache.get('events');
  if (cached) return res.json(cached);

  // Not cached — tell client to use SSE stream instead
  res.status(202).json({ streaming: true });
});

// SSE streaming endpoint: emits events source-by-source as they resolve
app.get('/api/events/stream', async (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  const send = (event, data) => {
    res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
  };

  // If already cached, stream entire cache immediately and close
  const cached = cache.get('events');
  if (cached) {
    send('events', { source: 'cache', events: cached });
    send('done', { total: cached.length });
    return res.end();
  }

  // Scrape each source and stream as it resolves
  const allEvents = [];
  let completed = 0;

  await Promise.allSettled(
    SOURCES.map(async ({ key, label, fn }) => {
      try {
        const events = await fn();
        // Enrich coords for this batch
        const missing = events.filter(e => !e.lat || !e.lng).slice(0, 5);
        if (missing.length) await enrichCoords(missing);
        allEvents.push(...events);
        completed++;
        send('events', { source: key, label, events });
      } catch (e) {
        completed++;
        send('error', { source: key, message: e.message });
      }
    })
  );

  cache.set('events', allEvents);
  saveDiskCache(allEvents);
  console.log(`Fetched ${allEvents.length} events total`);
  send('done', { total: allEvents.length });
  res.end();

  // Background: enrich TheList events with Songkick URLs, then update cache
  const theListEvents = allEvents.filter(e => e.id.startsWith('thelist-'));
  enrichSongkickUrls(theListEvents).then(() => {
    cache.set('events', allEvents);
    saveDiskCache(allEvents);
  }).catch(() => {});
});

app.get('/health', (req, res) => res.json({ ok: true }));

app.get('/api/venues', (req, res) => {
  res.json(VENUE_COORDS);
});

// ─── Songkick URL resolver ────────────────────────────────────────────────────
// Maps TheList venue slugs to human-readable names for Songkick search matching
const SONGKICK_VENUE_NAMES = {
  'fillmore':                 'fillmore',
  'warfield':                 'warfield',
  'independent':              'independent',
  'great_american_music_hall':'great american music hall',
  'chapel':                   'chapel',
  'bottom_of_the_hill':       'bottom of the hill',
  'brick_and_mortar':         'brick and mortar',
  'cafe_du_nord':             'cafe du nord',
  'swedish_american_hall':    'swedish american hall',
  'dna_lounge':               'dna lounge',
  'great_northern':           'great northern',
  'august_hall':              'august hall',
  'regency_ballroom':         'regency ballroom',
  'masonic':                  'masonic',
  'castro_theater':           'castro theatre',
  'castro_theataer':          'castro theatre',
  'rickshaw_stop':            'rickshaw stop',
  'rickshas_stop':            'rickshaw stop',
  'rickshaw_shop':            'rickshaw stop',
  'make-out_room':            'make-out room',
  'knockout':                 'knockout',
  'thee_stork_club':          'stork club',
  'yoshi\'s':                 'yoshis',
  'freight':                  'freight',
  'uc_theater':               'uc theatre',
  'greek_theatre':            'greek theatre',
  'fox_theater':              'fox theater oakland',
  'fox_theataer':             'fox theater oakland',
  'paramount_theatre':        'paramount theatre',
  'guild_theater':            'guild theater',
  'ivy_room':                 'ivy room',
  'catalyst':                 'catalyst',
  'moe\'s_alley':             'moe\'s alley',
  'moe_s_alley':              'moe\'s alley',
  'sweetwater_music_hall':    'sweetwater music hall',
  'mystic_theater':           'mystic theater',
  'cornerstone':              'cornerstone',
  'slim_s':                   'slim\'s',
  'independent':              'the independent',
  'hopmonk':                  'hopmonk',
  'hopmonk_tavern':           'hopmonk',
  'shoreline_amphitheatre':   'shoreline amphitheatre',
  'shoreline_ampheater':      'shoreline amphitheatre',
  'greek_theatre':            'greek theatre berkeley',
  'frost_amphitheater':       'frost amphitheater',
  'mountain_winery':          'mountain winery',
  'san_jose_civic':           'san jose civic',
  'hammer_theater_center':    'hammer theater',
};


app.get('/api/refresh', async (req, res) => {
  cache.del('events');
  try { writeFileSync(DISK_CACHE_PATH, JSON.stringify({ ts: 0, events: [] })); } catch (_) {}
  res.json({ ok: true, message: 'Cache cleared' });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Bay Area Events server running at http://localhost:${PORT}`);
});
