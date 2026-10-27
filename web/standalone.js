/* Standalone runtime for the free GitHub Pages copy.
   In Claude the app gets its database and tools from window.claude. Here, the same
   interface is served from data.json, which a GitHub Action rebuilds every hour.
   - Shared data (roles, companies, alerts, run logs) is read-only and refreshed every 10 minutes.
   - Your own changes (tracker, notes, saved views, settings) stay in this browser (localStorage).
   - Claude-only features (AI analysis, drafts, resume reading, web finders, Run now) are not available. */
(function () {
  if (window.claude && window.claude.use) return;           // opened inside Claude: use the real runtime
  var APP = document.documentElement.getAttribute('data-app') || 'app';
  var KEY = 'standalone:' + APP + ':writes';
  var base = {}, store = {}, subs = [], ready = null, builtAt = null;

  function readWrites() { try { return JSON.parse(localStorage.getItem(KEY) || '{}') || {}; } catch (e) { return {}; } }
  function saveWrite(path, doc) {
    var w = readWrites(); w[path] = doc === undefined ? null : doc;
    try { localStorage.setItem(KEY, JSON.stringify(w)); } catch (e) { console.warn('Could not save locally', e); }
  }
  function rebuild() {
    store = {};
    for (var k in base) store[k] = base[k];
    var w = readWrites();
    for (var p in w) { if (w[p] === null) delete store[p]; else store[p] = w[p]; }
  }
  function load() {
    return fetch('data.json?t=' + Date.now(), { cache: 'no-store' }).then(function (r) {
      if (!r.ok) throw new Error('data.json ' + r.status);
      return r.json();
    }).then(function (data) {
      var b = {};
      builtAt = data._built_at || null;
      for (var c in data) { if (c[0] === '_') continue; var docs = data[c]; for (var id in docs) b[c + '/' + id] = docs[id]; }
      base = b; rebuild(); notify();
    });
  }
  function notify() { subs.slice().forEach(function (s) { try { s(); } catch (e) { console.warn(e); } }); }
  function clone(d) { return d == null ? d : JSON.parse(JSON.stringify(d)); }
  function lastId(path) { return path.split('/').pop(); }
  function newId() { return Date.now().toString(36) + Math.random().toString(36).slice(2, 8); }

  function colDocs(path) {
    var pre = path + '/';
    return Object.keys(store).filter(function (k) { return k.indexOf(pre) === 0 && k.slice(pre.length).indexOf('/') < 0; })
      .map(function (k) { return { id: k.slice(pre.length), exists: true, data: function () { return clone(store[k]); }, metadata: {} }; });
  }
  function query(path, ord, lim) {
    function snap() {
      var docs = colDocs(path);
      if (ord) docs.sort(function (a, b) {
        var x = a.data()[ord[0]] || '', y = b.data()[ord[0]] || '';
        return (x > y ? 1 : x < y ? -1 : 0) * (ord[1] === 'desc' ? -1 : 1);
      });
      if (lim) docs = docs.slice(0, lim);
      return { docs: docs, size: docs.length, empty: !docs.length, docChanges: function () { return []; }, metadata: {} };
    }
    return {
      where: function () { return this; },
      orderBy: function (f, d) { return query(path, [f, d], lim); },
      limit: function (n) { return query(path, ord, n); },
      get: function () { return Promise.resolve(snap()); },
      onSnapshot: function (next) {
        var fire = function () { next(snap()); }; subs.push(fire); setTimeout(fire, 0);
        return function () { var i = subs.indexOf(fire); if (i >= 0) subs.splice(i, 1); };
      }
    };
  }
  function docSnap(path) { return { exists: path in store, data: function () { return clone(store[path]); }, id: lastId(path), metadata: {} }; }
  function doc(path) {
    return {
      id: lastId(path), path: path,
      get: function () {
        if (!(path in store) && path.indexOf('job_text/') === 0) {
          return fetch('text/' + encodeURIComponent(lastId(path)) + '.json').then(function (r) { return r.ok ? r.json() : null; })
            .then(function (d) { if (d) { base[path] = d; store[path] = d; } return docSnap(path); }, function () { return docSnap(path); });
        }
        return Promise.resolve(docSnap(path));
      },
      set: function (d) { store[path] = clone(d); saveWrite(path, store[path]); notify(); return Promise.resolve(); },
      update: function (d) { var n = Object.assign({}, store[path] || {}, clone(d)); store[path] = n; saveWrite(path, n); notify(); return Promise.resolve(); },
      delete: function () { delete store[path]; saveWrite(path, undefined); notify(); return Promise.resolve(); },
      onSnapshot: function (next) {
        var fire = function () { next(docSnap(path)); }; subs.push(fire); setTimeout(fire, 0);
        return function () { var i = subs.indexOf(fire); if (i >= 0) subs.splice(i, 1); };
      },
      collection: function (c) { return coll(path + '/' + c); }
    };
  }
  function coll(p) { return Object.assign(query(p), { doc: function (id) { return doc(p + '/' + (id || newId())); } }); }
  var db = { doc: doc, collection: coll };
  var user = {
    id: function () { return Promise.resolve('local'); },
    isOwner: function () { return Promise.resolve(true); },
    canEdit: function () { return Promise.resolve(true); }
  };
  var downloads = {
    save: function (o) {
      var data = o && o.data, type = /\.json$/i.test(o.filename) ? 'application/json' : /\.csv$/i.test(o.filename) ? 'text/csv' : 'text/plain';
      var blob = data instanceof Blob ? data : new Blob([data], { type: type + ';charset=utf-8' });
      var a = document.createElement('a'); a.href = URL.createObjectURL(blob); a.download = o.filename || 'download';
      document.body.appendChild(a); a.click(); setTimeout(function () { URL.revokeObjectURL(a.href); a.remove(); }, 1000);
      return Promise.resolve();
    }
  };
  ready = load().catch(function (e) { console.error(e); throw e; });
  setInterval(function () { if (!document.hidden) load().catch(function () {}); }, 10 * 60 * 1000);
  document.addEventListener('visibilitychange', function () { if (!document.hidden) load().catch(function () {}); });

  window.claude = {
    standalone: true,
    use: function (name) {
      if (name === 'db') return ready.then(function () { return db; });
      if (name === 'user') return Promise.resolve(user);
      if (name === 'downloads') return Promise.resolve(downloads);
      return Promise.resolve(null);                          // sample, mcp: Claude only
    }
  };
  window.standaloneInfo = function () { return { builtAt: builtAt }; };
})();
