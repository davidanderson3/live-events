// File: js/buttonStyles.js

// Detect an “icon-only” button
function isIconButton(btn) {
  // 1) if you've explicitly marked it with one of our icon classes
  if (btn.classList.contains('icon-button') ||
      btn.classList.contains('icon-btn') ||
      btn.classList.contains('circle-btn')) {
    return true;
  }

  // 2) or if its text is 1–2 non-alphanumeric chars (likely an emoji)
  const txt = btn.textContent.trim();
  return txt.length > 0 && txt.length <= 2 && /[^\w\s]/u.test(txt);
}

// Apply styling to a single button (unless it’s detected as an icon)
function styleButton(btn) {
  // 0️⃣ Don’t restyle our list-selector tabs
  if (btn.classList.contains('list-tab')
      || btn.closest('#listTabs')) {
    return;
  }

  // 1️⃣ Skip icon-only buttons or tag-filter buttons
  if (isIconButton(btn)) return;
  if (btn.classList.contains('tag-filter-button')) return;
  if (btn.classList.contains('genre-filter-btn')) return;
  if (btn.classList.contains('genre-filter-chip-remove')) return;

  // 2️⃣ Style everything else
  btn.style.border = 'none';
  btn.style.cursor = 'pointer';
}


// Style all existing buttons on the page
function styleAllButtons(root = document) {
  root.querySelectorAll('button').forEach(styleButton);
}

// Watch for buttons added dynamically
function observeNewButtons() {
  const obs = new MutationObserver(muts => {
    for (const m of muts) {
      m.addedNodes.forEach(node => {
        if (node.nodeType !== Node.ELEMENT_NODE) return;
        if (node.tagName === 'BUTTON') styleButton(node);
        if (node.querySelectorAll) {
          node.querySelectorAll('button').forEach(styleButton);
        }
      });
    }
  });
  obs.observe(document.body, { childList: true, subtree: true });
}

// Public initializer
export function initButtonStyles() {
  styleAllButtons();
  observeNewButtons();
}
