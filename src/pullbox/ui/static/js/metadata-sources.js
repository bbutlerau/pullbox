function metadataSourceSettings(seed, csrf) {
  const clone = value => JSON.parse(JSON.stringify(value));
  const labels = {
    comicvine_local: 'ComicVine Local Catalog', comicvine_api: 'ComicVine API',
    metron_api: 'Metron', gcd_local: 'GCD Local Database', gcd_api_v2: 'GCD API v2',
  };
  return {
    sources: clone(seed), savedSources: clone(seed), order: [], domainOrders: {},
    domains: {core: 'Core metadata', issues: 'Issue catalogs', artwork: 'Artwork', story_arcs: 'Story arcs'},
    labels, saving: false, testing: null, message: '', error: '', conflict: false,
    healthMessage: '', controllers: new Set(), alive: true, savedOrder: '', refreshing: false,
    init() { this.accept(seed); },
    destroy() { this.alive = false; this.controllers.forEach(controller => controller.abort()); },
    eligible(domain) {
      return this.order.filter(source => domain !== 'artwork' || !source.startsWith('gcd_'));
    },
    accept(data) {
      this.sources = clone(data);
      this.savedSources = clone(data);
      this.order = data.map(item => item.source);
      this.domainOrders = {};
      for (const domain of Object.keys(this.domains)) {
        if (data.some(item => Object.hasOwn(item.domain_priorities, domain))) {
          this.domainOrders[domain] = this.eligible(domain).sort((a, b) => {
            const left = data.find(item => item.source === a);
            const right = data.find(item => item.source === b);
            return (left.domain_priorities[domain] ?? left.priority) -
              (right.domain_priorities[domain] ?? right.priority) || a.localeCompare(b);
          });
        }
      }
      this.savedOrder = this.signature();
    },
    signature() {
      return JSON.stringify({order: this.order, domain_orders: Object.fromEntries(
        Object.keys(this.domains).filter(domain => this.domainOrders[domain]).map(domain => [domain, this.domainOrders[domain]])
      )});
    },
    get dirty() {
      return this.signature() !== this.savedOrder;
    },
    move(domain, index, direction) {
      if (this.saving) return;
      const order = domain === 'global' ? this.order : this.domainOrders[domain];
      const target = index + direction;
      if (!order || target < 0 || target >= order.length) return;
      const focused = document.activeElement;
      const row = focused?.closest('[data-source-row]');
      [order[index], order[target]] = [order[target], order[index]];
      this.message = '';
      if (row && this.$root.contains(row)) {
        this.$nextTick(() => {
          const button = focused.disabled ? row.querySelector('button:not(:disabled)') : focused;
          button?.focus({preventScroll: true});
        });
      }
    },
    toggleDomain(domain, enabled) {
      if (this.saving) return;
      if (enabled) this.domainOrders[domain] = this.eligible(domain);
      else delete this.domainOrders[domain];
      this.message = '';
    },
    reset() {
      if (this.saving) return;
      this.accept(this.savedSources);
      this.message = ''; this.error = ''; this.conflict = false;
    },
    async request(url, options = {}) {
      const controller = new AbortController();
      this.controllers.add(controller);
      const timeout = setTimeout(() => controller.abort(), 20000);
      try {
        const response = await fetch(url, {
          ...options, signal: controller.signal,
          headers: {'Content-Type': 'application/json', 'X-CSRF-Token': csrf},
        });
        const data = await response.json();
        return {ok: response.ok, status: response.status, data};
      } finally {
        clearTimeout(timeout);
        this.controllers.delete(controller);
      }
    },
    async save() {
      if (this.saving || this.testing || this.refreshing || !this.dirty) return;
      this.saving = true; this.error = ''; this.message = ''; this.conflict = false;
      try {
        const response = await this.request('/api/v1/metadata/priorities', {
          method: 'PUT', body: JSON.stringify({
            order: this.order, domain_orders: this.domainOrders,
            revisions: Object.fromEntries(this.savedSources.map(item => [item.source, item.revision])),
          }),
        });
        if (response.status === 409) {
          this.conflict = true;
          this.error = 'Settings changed in another session. Your draft is kept. Load saved priority before trying again.';
          return;
        }
        if (!response.ok) throw new Error('save');
        const data = response.data;
        if (!this.alive) return;
        this.accept(data);
        this.message = 'Metadata priority saved.';
      } catch (_) {
        if (this.alive) this.error = 'Could not save metadata priority. Your draft is kept; check the connection and retry.';
      } finally { this.saving = false; }
    },
    async reload() {
      if (this.saving || this.testing) return;
      this.saving = true;
      try {
        const response = await this.request('/api/v1/metadata/sources');
        if (!response.ok) throw new Error('load');
        const data = response.data;
        if (!this.alive) return;
        this.accept(data); this.error = ''; this.conflict = false; this.message = '';
      } catch (_) {
        if (this.alive) this.error = 'Could not load saved priority. Your draft is kept; check the connection and retry.';
      } finally { this.saving = false; }
    },
    status(item) {
      if (!item.capabilities.length && item.availability !== 'feature_disabled') return 'Not available in this build';
      const state = item.availability || item.last_status || 'not_checked';
      return {
        ok: 'Ready', empty: 'Ready', disabled: 'Disabled', feature_disabled: 'Not released yet',
        not_implemented: 'Not available in this build', unconfigured: 'Not configured',
        invalid_configuration: 'Configuration needs attention', authentication_failed: 'Authentication required',
        rate_limited: 'Rate limited', timeout: 'Timed out', unavailable: 'Unavailable',
        incompatible_response: 'Unexpected provider response', unsupported: 'Not supported',
        not_checked: 'Not checked yet',
      }[state] || 'Needs attention';
    },
    canTest(item) { return item.enabled && item.capabilities.length > 0 && !item.availability; },
    async refreshHealth() {
      if (this.refreshing) return;
      this.refreshing = true;
      try {
        const response = await this.request('/api/v1/metadata/sources');
        if (!response.ok) throw new Error('load');
        if (this.alive) this.sources = response.data;
      } catch (_) {
        if (this.alive) this.healthMessage = 'Could not refresh source status. Reload Metadata settings to check the saved configuration.';
      } finally { this.refreshing = false; }
    },
    async test(item) {
      if (this.testing || this.saving || this.refreshing || !this.canTest(item)) return;
      this.testing = item.source;
      this.healthMessage = 'Checking ' + this.labels[item.source] + '...';
      try {
        const response = await this.request('/api/v1/metadata/sources/' + item.source + '/test', {method: 'POST'});
        if (!response.ok) throw new Error('test');
        const data = response.data;
        if (!this.alive) return;
        if (!data.recorded) {
          this.healthMessage = 'Settings changed during this check. Test the saved configuration again.';
          return;
        }
        item.last_status = data.outcome.status;
        this.healthMessage = this.labels[item.source] + ': ' + this.status(item) + '.';
        if (data.outcome.retry_after_seconds != null) {
          this.healthMessage += ' Retry in ' + data.outcome.retry_after_seconds + ' seconds.';
        }
      } catch (_) {
        if (this.alive) this.healthMessage = 'Could not check ' + this.labels[item.source] + '. Check the connection and retry.';
      } finally { this.testing = null; }
    },
  };
}
