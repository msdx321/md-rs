export const presets = {
  resolution: [[0, 'No filtering'], [360, '360p'], [480, '480p'], [720, '720p · HD'], [1080, '1080p · Full HD'], [1440, '1440p · QHD'], [2160, '2160p · 4K']],
  speed: [1, 5, 10, 25, 50, 100].map(value => [value, `${value} MB/s`]),
  concurrency: [1, 2, 4, 8, 16, 32, 64, 128].map(value => [value, String(value)]),
  pages: [1, 3, 5, 10, 20].map(value => [value, `${value} ${value === 1 ? 'page' : 'pages'}`]),
};

// Keep the original input as the source of truth for serialization and validation.
// Call sync after loading or discarding values.
export function bindSettingPresets(fields) {
  const controls = Object.entries(fields).map(([id, options]) => {
    const input = document.getElementById(id);
    const label = document.querySelector(`label[for="${id}"]`);
    const select = document.createElement('select');
    select.id = `${id}-preset`;
    label.htmlFor = select.id;
    if (input.hasAttribute('aria-describedby')) select.setAttribute('aria-describedby', input.getAttribute('aria-describedby'));
    for (const [value, text] of options) {
      if (input.type === 'number' && value !== ''
        && ((input.min !== '' && Number(value) < Number(input.min))
          || (input.max !== '' && Number(value) > Number(input.max)))) continue;
      select.add(new Option(text, String(value)));
    }
    select.add(new Option('Custom…', 'custom'));
    const custom = document.createElement('div');
    custom.className = 'preset-custom';
    const customLabel = document.createElement('label');
    customLabel.htmlFor = id;
    customLabel.textContent = `Custom ${label.textContent.toLowerCase()}`;
    input.before(select, custom);
    custom.append(customLabel, input);
    const showCustom = () => { custom.hidden = select.value !== 'custom'; };
    select.addEventListener('change', () => {
      showCustom();
      if (select.value === 'custom') {
        input.focus();
        input.select();
      } else {
        input.value = select.value;
        input.dispatchEvent(new Event('input', { bubbles: true }));
        input.dispatchEvent(new Event('change', { bubbles: true }));
      }
    });
    input.addEventListener('invalid', () => {
      select.value = 'custom';
      showCustom();
      for (let parent = input.parentElement; parent; parent = parent.parentElement) {
        if (parent.tagName === 'DETAILS') parent.open = true;
      }
    });
    return { input, select, showCustom };
  });
  return () => {
    for (const { input, select, showCustom } of controls) {
      select.value = [...select.options].some(option => option.value !== 'custom' && option.value === input.value)
        ? input.value : 'custom';
      showCustom();
    }
  };
}
