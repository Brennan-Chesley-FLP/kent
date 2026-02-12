/**
 * Debug Palette - XPath/CSS Selector Debugger
 *
 * Floating panel for debugging scraper selectors and viewing continuation output.
 */

(function() {
  'use strict';

  // =============================================================================
  // Color Palette for Highlighting
  // =============================================================================

  const HIGHLIGHT_COLORS = [
    '#ff6b6b',  // Red
    '#4ecdc4',  // Teal
    '#ffe66d',  // Yellow
    '#95e1d3',  // Mint
    '#f38181',  // Coral
    '#aa96da',  // Lavender
    '#fcbad3',  // Pink
    '#a8d8ea',  // Sky Blue
    '#f9ed69',  // Lemon
    '#b8de6f',  // Lime
    '#ffb347',  // Orange
    '#77dd77',  // Pastel Green
  ];

  // =============================================================================
  // Debug Palette Class
  // =============================================================================

  class DebugPalette {
    constructor(config) {
      this.config = config;
      this.data = null;
      this.activeTab = 'selectors';
      this.minimized = false;

      // Color management
      this.colorAssignments = new Map(); // elementId -> colorIndex
      this.availableColors = [...Array(HIGHLIGHT_COLORS.length).keys()];

      // Dragging state
      this.isDragging = false;
      this.dragOffset = { x: 0, y: 0 };

      // Load saved position
      this.position = this.loadPosition();

      this.init();
    }

    async init() {
      this.createPalette();
      this.attachEventListeners();
      await this.fetchData();
    }

    // =========================================================================
    // Data Fetching
    // =========================================================================

    async fetchData() {
      this.showLoading();

      try {
        const response = await fetch(this.config.outputUrl);
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        this.data = await response.json();
        this.render();
      } catch (error) {
        this.showError(error.message);
      }
    }

    // =========================================================================
    // Palette Creation
    // =========================================================================

    createPalette() {
      const root = document.getElementById('debug-palette-root');
      if (!root) return;

      root.innerHTML = `
        <div id="debug-palette" style="top: ${this.position.top}px; right: ${this.position.right}px;">
          <div class="debug-palette-header">
            <span class="debug-palette-title">Debug Palette</span>
            <div class="debug-palette-controls">
              <button class="debug-palette-btn" id="debug-palette-minimize" title="Minimize">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                  <line x1="5" y1="12" x2="19" y2="12"></line>
                </svg>
              </button>
              <button class="debug-palette-btn" id="debug-palette-close" title="Close">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                  <line x1="18" y1="6" x2="6" y2="18"></line>
                  <line x1="6" y1="6" x2="18" y2="18"></line>
                </svg>
              </button>
            </div>
          </div>
          <div class="debug-palette-content">
            <div class="debug-palette-tabs">
              <button class="debug-palette-tab active" data-tab="selectors">Selectors</button>
              <button class="debug-palette-tab" data-tab="outputs">Outputs</button>
            </div>
            <div class="debug-palette-panel active" id="panel-selectors">
              <div class="debug-palette-loading">
                <div class="debug-palette-spinner"></div>
                Loading...
              </div>
            </div>
            <div class="debug-palette-panel" id="panel-outputs">
              <div class="debug-palette-loading">
                <div class="debug-palette-spinner"></div>
                Loading...
              </div>
            </div>
          </div>
        </div>
      `;

      this.palette = document.getElementById('debug-palette');
      this.header = this.palette.querySelector('.debug-palette-header');
    }

    // =========================================================================
    // Event Listeners
    // =========================================================================

    attachEventListeners() {
      // Tab switching
      this.palette.querySelectorAll('.debug-palette-tab').forEach(tab => {
        tab.addEventListener('click', (e) => this.switchTab(e.target.dataset.tab));
      });

      // Minimize button
      document.getElementById('debug-palette-minimize').addEventListener('click', () => {
        this.toggleMinimize();
      });

      // Close button
      document.getElementById('debug-palette-close').addEventListener('click', () => {
        this.palette.style.display = 'none';
      });

      // Dragging
      this.header.addEventListener('mousedown', (e) => this.startDrag(e));
      document.addEventListener('mousemove', (e) => this.drag(e));
      document.addEventListener('mouseup', () => this.endDrag());
    }

    // =========================================================================
    // Tab Management
    // =========================================================================

    switchTab(tabName) {
      this.activeTab = tabName;

      // Update tab buttons
      this.palette.querySelectorAll('.debug-palette-tab').forEach(tab => {
        tab.classList.toggle('active', tab.dataset.tab === tabName);
      });

      // Update panels
      this.palette.querySelectorAll('.debug-palette-panel').forEach(panel => {
        panel.classList.toggle('active', panel.id === `panel-${tabName}`);
      });
    }

    toggleMinimize() {
      this.minimized = !this.minimized;
      this.palette.classList.toggle('minimized', this.minimized);
    }

    // =========================================================================
    // Dragging
    // =========================================================================

    startDrag(e) {
      if (e.target.closest('.debug-palette-btn')) return;

      this.isDragging = true;
      const rect = this.palette.getBoundingClientRect();
      this.dragOffset = {
        x: e.clientX - rect.left,
        y: e.clientY - rect.top
      };
      this.header.style.cursor = 'grabbing';
    }

    drag(e) {
      if (!this.isDragging) return;

      const x = e.clientX - this.dragOffset.x;
      const y = e.clientY - this.dragOffset.y;

      // Keep within viewport
      const maxX = window.innerWidth - this.palette.offsetWidth;
      const maxY = window.innerHeight - this.palette.offsetHeight;

      const newX = Math.max(0, Math.min(x, maxX));
      const newY = Math.max(0, Math.min(y, maxY));

      this.palette.style.left = `${newX}px`;
      this.palette.style.top = `${newY}px`;
      this.palette.style.right = 'auto';
    }

    endDrag() {
      if (!this.isDragging) return;

      this.isDragging = false;
      this.header.style.cursor = 'move';
      this.savePosition();
    }

    savePosition() {
      const rect = this.palette.getBoundingClientRect();
      const position = {
        top: rect.top,
        right: window.innerWidth - rect.right
      };
      localStorage.setItem('debugPalettePosition', JSON.stringify(position));
    }

    loadPosition() {
      try {
        const saved = localStorage.getItem('debugPalettePosition');
        if (saved) {
          return JSON.parse(saved);
        }
      } catch (e) {
        // Ignore
      }
      return { top: 20, right: 20 };
    }

    // =========================================================================
    // Rendering
    // =========================================================================

    showLoading() {
      ['selectors', 'outputs'].forEach(panel => {
        const el = document.getElementById(`panel-${panel}`);
        if (el) {
          el.innerHTML = `
            <div class="debug-palette-loading">
              <div class="debug-palette-spinner"></div>
              Loading...
            </div>
          `;
        }
      });
    }

    showError(message) {
      const content = this.palette.querySelector('.debug-palette-content');
      content.innerHTML = `
        <div class="debug-palette-error">
          <div class="debug-palette-error-title">Error loading data</div>
          <div class="debug-palette-error-message">${this.escapeHtml(message)}</div>
        </div>
      `;
    }

    render() {
      if (!this.data) return;

      this.renderSelectors();
      this.renderOutputs();

      // Show error if present
      if (this.data.error) {
        const errorHtml = `
          <div class="debug-palette-error">
            <div class="debug-palette-error-title">Continuation Error</div>
            <div class="debug-palette-error-message">${this.escapeHtml(this.data.error)}</div>
          </div>
        `;
        document.getElementById('panel-selectors').insertAdjacentHTML('afterbegin', errorHtml);
      }
    }

    renderSelectors() {
      const panel = document.getElementById('panel-selectors');
      if (!panel) return;

      if (!this.data.selectors || this.data.selectors.length === 0) {
        panel.innerHTML = '<div style="padding: 20px; color: #6b7280; text-align: center;">No selectors found</div>';
        return;
      }

      panel.innerHTML = `
        <ul class="selector-tree">
          ${this.data.selectors.map(s => this.renderSelectorItem(s)).join('')}
        </ul>
      `;

      // Attach checkbox listeners
      panel.querySelectorAll('.selector-checkbox').forEach(checkbox => {
        checkbox.addEventListener('change', (e) => {
          const item = e.target.closest('.selector-item');
          const selector = item.dataset.selector;
          const selectorType = item.dataset.selectorType;
          const elementId = item.dataset.elementId;
          const parentElementId = item.dataset.parentElementId || null;

          this.toggleHighlight(elementId, selector, selectorType, e.target.checked, item, parentElementId);
        });
      });
    }

    renderSelectorItem(selector, depth = 0) {
      const statusIcon = selector.status === 'pass'
        ? '<span class="selector-status pass" title="Passed">&#10003;</span>'
        : '<span class="selector-status fail" title="Failed">&#10007;</span>';

      const sample = selector.sample_elements && selector.sample_elements.length > 0
        ? `<div class="selector-sample" title="${this.escapeHtml(selector.sample_elements[0])}">${this.escapeHtml(selector.sample_elements[0])}</div>`
        : '';

      const children = selector.children && selector.children.length > 0
        ? `<ul class="selector-tree">${selector.children.map(c => this.renderSelectorItem(c, depth + 1)).join('')}</ul>`
        : '';

      // Include parent_element_id if present for scoped highlighting
      const parentAttr = selector.parent_element_id
        ? `data-parent-element-id="${selector.parent_element_id}"`
        : '';

      return `
        <li class="selector-item" data-selector="${this.escapeHtml(selector.selector)}" data-selector-type="${selector.selector_type}" data-element-id="${selector.element_id}" ${parentAttr}>
          <div class="selector-header">
            <input type="checkbox" class="selector-checkbox">
            <div class="selector-color-swatch"></div>
            <div class="selector-info">
              <div class="selector-description">${this.escapeHtml(selector.description)}</div>
              <div class="selector-query">${this.escapeHtml(selector.selector)}</div>
              ${sample}
            </div>
            <div class="selector-stats">
              <span class="selector-count">${selector.match_count}</span>
              ${statusIcon}
            </div>
          </div>
          ${children}
        </li>
      `;
    }

    renderOutputs() {
      const panel = document.getElementById('panel-outputs');
      if (!panel) return;

      if (!this.data.yields || this.data.yields.length === 0) {
        panel.innerHTML = '<div style="padding: 20px; color: #6b7280; text-align: center;">No outputs</div>';
        return;
      }

      // Summary badges
      const summaryHtml = Object.entries(this.data.yield_summary)
        .map(([type, count]) => `
          <div class="output-badge">
            <span class="output-badge-count">${count}</span>
            <span class="output-badge-type">${type}</span>
          </div>
        `).join('');

      // Group yields by type
      const groups = {};
      this.data.yields.forEach(y => {
        if (!groups[y.type]) groups[y.type] = [];
        groups[y.type].push(y);
      });

      const groupsHtml = Object.entries(groups).map(([type, items]) => `
        <div class="output-group" data-type="${type}">
          <div class="output-group-header">
            <svg class="output-group-toggle" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <polyline points="9 18 15 12 9 6"></polyline>
            </svg>
            <span class="output-group-title">${type}</span>
            <span class="output-group-count">(${items.length})</span>
          </div>
          <div class="output-group-items">
            ${items.map(item => this.renderOutputItem(item)).join('')}
          </div>
        </div>
      `).join('');

      panel.innerHTML = `
        <div class="output-summary">${summaryHtml}</div>
        ${groupsHtml}
      `;

      // Attach group toggle listeners
      panel.querySelectorAll('.output-group-header').forEach(header => {
        header.addEventListener('click', () => {
          header.closest('.output-group').classList.toggle('expanded');
        });
      });
    }

    renderOutputItem(item) {
      let details = '';
      let preview = '';

      switch (item.type) {
        case 'ParsedData':
          details = `<span class="output-item-detail">${this.escapeHtml(item.data_type)}</span>`;
          if (item.preview) {
            preview = `<div class="output-item-preview">${this.escapeHtml(item.preview)}</div>`;
          }
          break;

        case 'NavigatingRequest':
        case 'NonNavigatingRequest':
          details = `
            <div class="output-item-detail">
              <strong>${item.method || 'GET'}</strong> ${this.escapeHtml(item.url || '')}<br>
              ${item.continuation ? `<strong>continuation:</strong> ${this.escapeHtml(item.continuation)}` : ''}
            </div>
          `;
          break;

        case 'ArchiveRequest':
          details = `
            <div class="output-item-detail">
              <strong>${item.method || 'GET'}</strong> ${this.escapeHtml(item.url || '')}<br>
              <strong>continuation:</strong> ${this.escapeHtml(item.continuation || 'N/A')}<br>
              <strong>expected_type:</strong> ${this.escapeHtml(item.expected_type || 'unknown')}
            </div>
          `;
          break;

        default:
          if (item.preview) {
            preview = `<div class="output-item-preview">${this.escapeHtml(item.preview)}</div>`;
          }
      }

      return `
        <div class="output-item ${item.type}">
          ${details}
          ${preview}
        </div>
      `;
    }

    // =========================================================================
    // Highlighting
    // =========================================================================

    toggleHighlight(elementId, selector, selectorType, enable, itemEl, parentElementId = null) {
      const swatch = itemEl.querySelector('.selector-color-swatch');

      if (enable) {
        // Assign a color
        const colorIndex = this.assignColor(elementId);
        if (colorIndex === null) {
          // No colors available
          itemEl.querySelector('.selector-checkbox').checked = false;
          return;
        }

        // Apply highlight to matching elements (scoped to parent if present)
        this.applyHighlight(selector, selectorType, colorIndex, parentElementId);

        // Update UI
        itemEl.classList.add('highlighted');
        swatch.style.backgroundColor = HIGHLIGHT_COLORS[colorIndex];
      } else {
        // Remove highlight
        const colorIndex = this.colorAssignments.get(elementId);
        if (colorIndex !== undefined) {
          this.removeHighlight(colorIndex);
          this.releaseColor(elementId);
        }

        // Update UI
        itemEl.classList.remove('highlighted');
        swatch.style.backgroundColor = '';
      }
    }

    assignColor(elementId) {
      if (this.availableColors.length === 0) {
        return null;
      }
      const colorIndex = this.availableColors.shift();
      this.colorAssignments.set(elementId, colorIndex);
      return colorIndex;
    }

    releaseColor(elementId) {
      const colorIndex = this.colorAssignments.get(elementId);
      if (colorIndex !== undefined) {
        this.colorAssignments.delete(elementId);
        this.availableColors.push(colorIndex);
        this.availableColors.sort((a, b) => a - b);
      }
    }

    applyHighlight(selector, selectorType, colorIndex, parentElementId = null) {
      const className = `debug-highlight-${colorIndex}`;
      const elements = this.findElements(selector, selectorType, parentElementId);

      elements.forEach(el => {
        el.classList.add(className);
      });
    }

    removeHighlight(colorIndex) {
      const className = `debug-highlight-${colorIndex}`;
      document.querySelectorAll(`.${className}`).forEach(el => {
        el.classList.remove(className);
      });
    }

    /**
     * Find the parent selector info by element ID.
     * Recursively searches through selectors and their children.
     */
    findSelectorById(elementId, selectors = null) {
      selectors = selectors || this.data.selectors;
      for (const sel of selectors) {
        if (sel.element_id === elementId) {
          return sel;
        }
        if (sel.children && sel.children.length > 0) {
          const found = this.findSelectorById(elementId, sel.children);
          if (found) return found;
        }
      }
      return null;
    }

    /**
     * Get the full selector chain for a scoped query.
     * Returns array of {selector, selectorType} from root to the given elementId.
     */
    getSelectorChain(elementId) {
      const chain = [];
      let currentId = elementId;

      while (currentId) {
        const selectorInfo = this.findSelectorById(currentId);
        if (!selectorInfo) break;

        chain.unshift({
          selector: selectorInfo.selector,
          selectorType: selectorInfo.selector_type
        });

        currentId = selectorInfo.parent_element_id;
      }

      return chain;
    }

    findElements(selector, selectorType, parentElementId = null) {
      const elements = [];

      try {
        if (parentElementId) {
          // Scoped query: find parent elements first, then run selector within each
          const selectorChain = this.getSelectorChain(parentElementId);

          // Start from document, apply each selector in the chain to get parent contexts
          let contexts = [document];

          for (const chainItem of selectorChain) {
            const newContexts = [];
            for (const ctx of contexts) {
              const found = this.findElementsInContext(
                chainItem.selector,
                chainItem.selectorType,
                ctx
              );
              newContexts.push(...found);
            }
            contexts = newContexts;
          }

          // Now apply the target selector within each parent context
          for (const parentEl of contexts) {
            const found = this.findElementsInContext(selector, selectorType, parentEl);
            elements.push(...found);
          }
        } else {
          // Non-scoped query: run from document root
          elements.push(...this.findElementsInContext(selector, selectorType, document));
        }
      } catch (e) {
        console.warn(`Failed to find elements for selector: ${selector}`, e);
      }

      return elements;
    }

    /**
     * Find elements matching a selector within a given context element.
     */
    findElementsInContext(selector, selectorType, context) {
      const elements = [];

      try {
        if (selectorType === 'xpath') {
          const result = document.evaluate(
            selector,
            context,
            null,
            XPathResult.ORDERED_NODE_SNAPSHOT_TYPE,
            null
          );
          for (let i = 0; i < result.snapshotLength; i++) {
            const node = result.snapshotItem(i);
            if (node.nodeType === Node.ELEMENT_NODE) {
              elements.push(node);
            }
          }
        } else if (selectorType === 'css') {
          if (context === document) {
            elements.push(...document.querySelectorAll(selector));
          } else {
            elements.push(...context.querySelectorAll(selector));
          }
        }
      } catch (e) {
        console.warn(`Failed to find elements for selector "${selector}" in context`, e);
      }

      return elements;
    }

    // =========================================================================
    // Utilities
    // =========================================================================

    escapeHtml(str) {
      if (!str) return '';
      const div = document.createElement('div');
      div.textContent = str;
      return div.innerHTML;
    }
  }

  // =============================================================================
  // Initialize
  // =============================================================================

  if (window.DEBUG_PALETTE_CONFIG) {
    window.debugPalette = new DebugPalette(window.DEBUG_PALETTE_CONFIG);
  }

})();
