// Column preferences belong to the table, so live row updates never reset them.
export function initTableColumns() {
  for (const table of document.querySelectorAll('table[data-resizable]')) {
    const headers = [...table.querySelectorAll('th')];
    const columns = [...table.querySelectorAll('col')];
    const handles = [];
    const minimums = headers.map(header => Number(header.dataset.minWidth || 90));
    const key = `media-downloader-columns-${table.dataset.resizable}`;
    const tools = document.createElement('div');
    tools.className = 'table-tools';
    const hint = document.createElement('span');
    hint.textContent = 'Drag column edges to resize';
    const reset = document.createElement('button');
    reset.type = 'button';
    reset.className = 'tiny';
    reset.textContent = 'Reset columns';
    reset.disabled = true;
    tools.append(hint, reset);
    table.parentElement.before(tools);

    function apply(widths) {
      const total = widths.reduce((sum, width) => sum + width, 0);
      table.style.width = '100%';
      table.style.minWidth = `${total}px`;
      // The last column fills spare space while the resizable columns keep their widths.
      columns.forEach((column, index) => { column.style.width = index === columns.length - 1 ? 'auto' : `${widths[index]}px`; });
      handles.forEach((handle, index) => handle.setAttribute('aria-valuenow', String(Math.round(widths[index]))));
      reset.disabled = false;
    }
    function remember(widths) {
      try { localStorage.setItem(key, JSON.stringify(widths)); } catch {}
    }
    function measure() { return headers.map(header => header.getBoundingClientRect().width); }
    reset.onclick = () => {
      table.style.removeProperty('width');
      table.style.removeProperty('min-width');
      columns.forEach(column => column.style.removeProperty('width'));
      try { localStorage.removeItem(key); } catch {}
      reset.disabled = true;
      updateMeasurements();
    };
    headers.slice(0, -1).forEach((header, index) => {
      const handle = document.createElement('span');
      handle.className = 'column-resizer';
      handle.tabIndex = 0;
      handle.setAttribute('role', 'separator');
      handle.setAttribute('aria-orientation', 'vertical');
      handle.setAttribute('aria-label', `Resize ${header.textContent.trim()} column`);
      handle.setAttribute('aria-valuemin', String(minimums[index]));
      handle.setAttribute('aria-valuemax', '1000');
      handle.setAttribute('aria-valuenow', String(Math.round(header.getBoundingClientRect().width) || minimums[index]));
      handle.title = 'Drag or use arrow keys to resize. Double-click or press Home to reset columns.';
      header.append(handle);
      handles.push(handle);
      let startX;
      let widths;
      let initialWidth;
      handle.addEventListener('focus', () => handle.setAttribute('aria-valuenow', String(Math.round(header.getBoundingClientRect().width))));
      handle.addEventListener('pointerdown', event => {
        if (event.button !== 0) return;
        event.preventDefault();
        handle.focus();
        widths = measure();
        initialWidth = widths[index];
        startX = event.clientX;
        handle.setPointerCapture(event.pointerId);
        handle.classList.add('resizing');
      });
      handle.addEventListener('pointermove', event => {
        if (startX === undefined) return;
        widths[index] = Math.min(1000, Math.max(minimums[index], initialWidth + event.clientX - startX));
        apply(widths);
      });
      handle.addEventListener('lostpointercapture', () => {
        if (startX === undefined) return;
        remember(widths);
        startX = undefined;
        handle.classList.remove('resizing');
      });
      handle.addEventListener('dblclick', () => reset.click());
      handle.addEventListener('keydown', event => {
        if (event.key === 'Home') { event.preventDefault(); reset.click(); return; }
        if (!['ArrowLeft', 'ArrowRight'].includes(event.key)) return;
        event.preventDefault();
        const next = measure();
        const delta = (event.key === 'ArrowRight' ? 1 : -1) * (event.shiftKey ? 40 : 10);
        next[index] = Math.min(1000, Math.max(minimums[index], next[index] + delta));
        apply(next);
        remember(next);
      });
    });
    try {
      const widths = JSON.parse(localStorage.getItem(key));
      if (Array.isArray(widths) && widths.length === columns.length && widths.every((width, index) => Number.isFinite(width) && width >= minimums[index] && width <= 1000)) apply(widths);
    } catch {}
    function updateMeasurements() {
      const widths = measure();
      handles.forEach((handle, index) => {
        const width = String(Math.round(widths[index]));
        if (width !== '0' && handle.getAttribute('aria-valuenow') !== width) handle.setAttribute('aria-valuenow', width);
      });
    }
    new ResizeObserver(updateMeasurements).observe(table);
  }
}
