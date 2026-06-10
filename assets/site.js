/* ============================================================
   site.js — theme, manifest rendering, markdown post viewer
   ============================================================ */

/* ---------- Theme ---------- */
(function () {
  const KEY = 'il-theme';
  const root = document.documentElement;
  const saved = localStorage.getItem(KEY);
  const sysDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
  root.setAttribute('data-theme', saved || (sysDark ? 'dark' : 'light'));

  window.toggleTheme = function () {
    const next = root.getAttribute('data-theme') === 'dark' ? 'light' : 'dark';
    root.setAttribute('data-theme', next);
    localStorage.setItem(KEY, next);
  };
})();

/* ---------- helpers ---------- */
function fmtDate(iso) {
  const d = new Date(iso + 'T00:00:00');
  return d.toLocaleDateString('en-US', { day: '2-digit', month: 'short', year: 'numeric' }).toUpperCase();
}
function escapeHtml(s) {
  return s.replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

/* ---------- Load manifest ---------- */
async function loadManifest() {
  try {
    const res = await fetch('posts/manifest.json', { cache: 'no-cache' });
    const data = await res.json();
    const posts = (data.posts || []).slice().sort((a, b) => b.date.localeCompare(a.date));
    return posts;
  } catch (e) {
    console.error('Could not load posts/manifest.json', e);
    return [];
  }
}

/* ---------- Featured (homepage) ---------- */
async function renderFeatured(targetId, n) {
  const el = document.getElementById(targetId);
  if (!el) return;
  const posts = await loadManifest();
  el.innerHTML = posts.slice(0, n).map((p, i) => `
    <a class="card feat-card" href="post.html?slug=${encodeURIComponent(p.slug)}">
      <div class="k">// ${String(i + 1).padStart(2, '0')}</div>
      <h3>${escapeHtml(p.title)}</h3>
      <p>${escapeHtml(p.excerpt || '')}</p>
      <div class="foot"><b>${fmtDate(p.date)}</b><span>${escapeHtml(p.readingTime || '')}</span></div>
    </a>`).join('');
}

/* ---------- Full list (blog page) ---------- */
async function renderPostList(targetId) {
  const el = document.getElementById(targetId);
  if (!el) return;
  const posts = await loadManifest();
  const allTags = [...new Set(posts.flatMap(p => p.tags || []))].sort();

  const filterEl = document.getElementById('tag-filter');
  if (filterEl) {
    filterEl.innerHTML =
      `<button class="tagfilter active" data-tag="*">all · ${posts.length}</button>` +
      allTags.map(t => `<button class="tagfilter" data-tag="${escapeHtml(t)}">#${escapeHtml(t)}</button>`).join('');
  }

  function paint(filter) {
    const list = filter === '*' ? posts : posts.filter(p => (p.tags || []).includes(filter));
    el.innerHTML = list.map(p => `
      <a class="post-row" href="post.html?slug=${encodeURIComponent(p.slug)}">
        <span class="pdate">${fmtDate(p.date)}</span>
        <span class="pmain">
          <span class="ptitle">${escapeHtml(p.title)}</span>
          <span class="pexc">${escapeHtml(p.excerpt || '')}</span>
          <span class="ptags">${(p.tags || []).map(t => `<span class="tag">#${escapeHtml(t)}</span>`).join('')}</span>
        </span>
        <span class="pmeta">${escapeHtml(p.readingTime || '')} <span class="arrow">→</span></span>
      </a>`).join('');
    if (!list.length) el.innerHTML = `<p style="font-family:var(--font-mono);color:var(--dim);padding:30px 18px">// no posts tagged #${escapeHtml(filter)}</p>`;
  }
  paint('*');

  if (filterEl) {
    filterEl.addEventListener('click', e => {
      const btn = e.target.closest('.tagfilter');
      if (!btn) return;
      filterEl.querySelectorAll('.tagfilter').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      paint(btn.dataset.tag);
    });
  }
}

/* ---------- Single post viewer ---------- */
async function renderPost() {
  const root = document.getElementById('post-root');
  if (!root) return;
  const slug = new URLSearchParams(location.search).get('slug');
  const posts = await loadManifest();
  const meta = posts.find(p => p.slug === slug);

  if (!meta) {
    root.innerHTML = `<div class="post-head"><h1 style="font-family:var(--font-mono)">404 · post not found</h1>
      <p style="color:var(--text-2)">No post with slug <code>${escapeHtml(slug || '')}</code>. <a href="blog.html" style="color:var(--accent)">← Back to all writing</a></p></div>`;
    return;
  }

  document.title = meta.title + ' · Iván Losada';

  marked.setOptions({ breaks: false, gfm: true });

  let md = '';
  try {
    const res = await fetch(meta.file, { cache: 'no-cache' });
    md = await res.text();
  } catch (e) {
    root.innerHTML = `<p style="color:var(--dim)">Could not load ${escapeHtml(meta.file)}</p>`;
    return;
  }

  // strip an optional leading H1 (title comes from manifest)
  md = md.replace(/^\s*#\s+.*\n/, '');

  const bodyHtml = marked.parse(md);

  root.innerHTML = `
    <div class="post-head">
      <a class="back" href="blog.html">← all writing</a>
      <div class="post-tags">${(meta.tags || []).map(t => `<span class="tag">#${escapeHtml(t)}</span>`).join('')}</div>
      <h1>${escapeHtml(meta.title)}</h1>
      <div class="post-byline">
        <span>${fmtDate(meta.date)}</span><span class="sep">·</span>
        <span>${escapeHtml(meta.readingTime || '')} read</span><span class="sep">·</span>
        <span>Iván Losada</span>
      </div>
    </div>
    <article class="prose">${bodyHtml}</article>
    <div class="post-foot">
      <a class="back" href="blog.html">← all writing</a>
      <a class="back" href="index.html">home →</a>
    </div>`;

  // build TOC
  buildTOC(root);

  // syntax highlight (marked v12 dropped the inline highlight option)
  root.querySelectorAll('pre code').forEach(block => {
    try { hljs.highlightElement(block); } catch (e) { /* noop */ }
  });

  // click-to-zoom lightbox for images
  root.querySelectorAll('.prose img').forEach(img => {
    img.classList.add('zoomable');
    img.addEventListener('click', () => openLightbox(img));
  });
}

/* ---------- Image lightbox ---------- */
function openLightbox(img) {
  const ov = document.createElement('div');
  ov.className = 'lightbox';
  const full = document.createElement('img');
  full.src = img.src;
  full.alt = img.alt || '';
  ov.appendChild(full);

  function close() {
    document.removeEventListener('keydown', onKey);
    document.body.style.overflow = '';
    ov.remove();
  }
  function onKey(e) { if (e.key === 'Escape') close(); }

  ov.addEventListener('click', close);
  document.addEventListener('keydown', onKey);
  document.body.style.overflow = 'hidden';
  document.body.appendChild(ov);
}

function buildTOC(root) {
  const toc = document.getElementById('toc');
  if (!toc) return;
  const heads = [...root.querySelectorAll('.prose h2, .prose h3')];
  if (heads.length < 3) { toc.style.display = 'none'; return; }
  heads.forEach((h, i) => { if (!h.id) h.id = 'h-' + i; });
  toc.innerHTML = `<div class="toc-lbl">// on this page</div>` +
    heads.map(h => `<a href="#${h.id}" class="toc-${h.tagName.toLowerCase()}">${escapeHtml(h.textContent)}</a>`).join('');

  const obs = new IntersectionObserver(entries => {
    entries.forEach(en => {
      if (en.isIntersecting) {
        toc.querySelectorAll('a').forEach(a => a.classList.toggle('active', a.getAttribute('href') === '#' + en.target.id));
      }
    });
  }, { rootMargin: '-10% 0px -75% 0px' });
  heads.forEach(h => obs.observe(h));
}

/* ---------- mobile nav ---------- */
function initBurger() {
  const b = document.querySelector('.nav-burger');
  const links = document.querySelector('.nav-links');
  if (!b || !links) return;
  b.addEventListener('click', () => {
    const open = links.classList.toggle('open');
    b.setAttribute('aria-expanded', String(open));
  });
  links.addEventListener('click', e => {
    if (e.target.closest('a')) {
      links.classList.remove('open');
      b.setAttribute('aria-expanded', 'false');
    }
  });
}
document.addEventListener('DOMContentLoaded', initBurger);
