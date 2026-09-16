(() => {
  const modes = ['light', 'dark', 'system'];
  const key = 'media-downloader-theme';
  let theme = 'light';
  try {
    const saved = localStorage.getItem(key);
    if (modes.includes(saved)) theme = saved;
  } catch {}

  function apply(value) {
    theme = value;
    document.documentElement.dataset.theme = theme;
    const button = document.getElementById('theme-toggle');
    if (button) {
      button.textContent = theme[0].toUpperCase() + theme.slice(1);
      button.setAttribute('aria-label', `Color theme: ${theme}. Change theme`);
    }
  }

  apply(theme);
  document.addEventListener('DOMContentLoaded', () => {
    apply(theme);
    document.getElementById('theme-toggle')?.addEventListener('click', () => {
      apply(modes[(modes.indexOf(theme) + 1) % modes.length]);
      try { localStorage.setItem(key, theme); } catch {}
    });
  });
  window.addEventListener('storage', (event) => {
    if (event.key === key) apply(modes.includes(event.newValue) ? event.newValue : 'light');
  });
})();
