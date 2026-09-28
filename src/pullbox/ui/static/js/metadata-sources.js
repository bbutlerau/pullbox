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
    metronBase: null, metronEnabled: false, metronToken: '', metronClear: false,
    metronSaving: false, metronError: '', metronMessage: '', metronConflict: false,
    healthRefreshPending: false,
    init() { this.accept(seed); this.acceptMetron(seed.find(item => item.source === 'metron_api')); },
    destroy() {
      this.alive = false; this.metronToken = '';
      this.controllers.forEach(controller => controller.abort());
    },
    get busy() { return this.saving || this.testing || this.refreshing || this.metronSaving; },
    get metronDirty() {
      return Boolean(this.metronBase && (this.metronEnabled !== this.metronBase.enabled ||
        this.metronToken || this.metronClear));
    },
    acceptMetron(item) {
      this.metronBase = item ? clone(item) : null;
      this.metronEnabled = Boolean(item?.enabled);
      this.metronToken = ''; this.metronClear = false;
      this.metronError = ''; this.metronMessage = ''; this.metronConflict = false;
    },
    async saveMetron() {
      if (this.busy || !this.metronDirty) return;
      this.metronError = ''; this.metronMessage = '';
      if (this.metronEnabled && this.metronClear) {
        this.metronError = 'Disable Metron before removing its saved token.';
        return;
      }
      if (this.metronEnabled && !this.metronToken && !this.metronBase.credential_configured) {
        this.metronError = 'Enter a token before enabling Metron.';
        return;
      }
      if (this.metronToken && (!/^[\x21-\x7e]+$/.test(this.metronToken) ||
          this.metronToken.length > 4096 || this.metronToken.startsWith('enc:'))) {
        this.metronError = 'Enter the API token from your Metron account, without spaces or line breaks.';
        return;
      }
      this.metronSaving = true;
      const base = clone(this.metronBase);
      try {
        const response = await this.request('/api/v1/metadata/sources/metron_api', {
          method: 'PUT', body: JSON.stringify({
            revision: base.revision, enabled: this.metronEnabled,
            priority: base.priority, domain_priorities: base.domain_priorities, settings: base.settings,
            ...(this.metronToken && !this.metronClear ? {credential: this.metronToken} : {}),
            clear_credential: this.metronClear,
          }),
        });
        if (!this.alive) return;
        if (response.status === 409) {
          this.metronConflict = true;
          this.metronError = 'Metron settings changed in another session. Your draft is kept. Load saved Metron settings before trying again.';
          return;
        }
        if (!response.ok) throw new Error('save');
        const policy = response.data;
        const current = this.sources.find(item => item.source === 'metron_api');
        const descriptor = {...current, ...policy, availability: policy.configuration_status ||
          (!policy.enabled ? 'disabled' : !policy.credential_configured ? 'unconfigured' : null)};
        this.sources = this.sources.map(item => item.source === 'metron_api' ? descriptor : item);
        // Only advance the priority revision when that draft saw the same policy.
        this.savedSources = this.savedSources.map(item =>
          item.source === 'metron_api' && item.revision === base.revision ? clone(descriptor) : item);
        this.acceptMetron(policy);
        this.healthMessage = '';
        this.metronMessage = 'Metron settings saved. Connection checks use this saved configuration.';
      } catch (_) {
        if (this.alive) this.metronError = 'Could not save Metron settings. Your draft is kept; check the connection and retry.';
      } finally { this.metronSaving = false; this.flushHealthRefresh(); }
    },
    async reloadMetron() {
      if (this.busy) return;
      this.metronSaving = true;
      try {
        const response = await this.request('/api/v1/metadata/sources');
        if (!response.ok) throw new Error('load');
        if (!this.alive) return;
        this.sources = response.data;
        this.acceptMetron(response.data.find(item => item.source === 'metron_api'));
      } catch (_) {
        if (this.alive) this.metronError = 'Could not load Metron settings. Your draft is kept; check the connection and retry.';
      } finally { this.metronSaving = false; this.flushHealthRefresh(); }
    },
    flushHealthRefresh() {
      if (this.alive && this.healthRefreshPending && !this.busy) {
        this.healthRefreshPending = false;
        this.refreshHealth();
      }
    },
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
      if (this.busy) return;
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
      if (this.busy) return;
      if (enabled) this.domainOrders[domain] = this.eligible(domain);
      else delete this.domainOrders[domain];
      this.message = '';
    },
    reset() {
      if (this.busy) return;
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
      if (this.busy || !this.dirty) return;
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
        const previous = this.savedSources.find(item => item.source === 'metron_api');
        if (this.metronBase?.revision === previous?.revision) {
          this.metronBase = clone(data.find(item => item.source === 'metron_api'));
        }
        this.accept(data);
        this.message = 'Metadata priority saved.';
      } catch (_) {
        if (this.alive) this.error = 'Could not save metadata priority. Your draft is kept; check the connection and retry.';
      } finally { this.saving = false; this.flushHealthRefresh(); }
    },
    async reload() {
      if (this.busy) return;
      this.saving = true;
      try {
        const response = await this.request('/api/v1/metadata/sources');
        if (!response.ok) throw new Error('load');
        const data = response.data;
        if (!this.alive) return;
        this.accept(data); this.error = ''; this.conflict = false; this.message = '';
      } catch (_) {
        if (this.alive) this.error = 'Could not load saved priority. Your draft is kept; check the connection and retry.';
      } finally { this.saving = false; this.flushHealthRefresh(); }
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
      if (this.busy) { this.healthRefreshPending = true; return; }
      this.refreshing = true;
      try {
        const response = await this.request('/api/v1/metadata/sources');
        if (!response.ok) throw new Error('load');
        if (this.alive) this.sources = response.data;
      } catch (_) {
        if (this.alive) this.healthMessage = 'Could not refresh source status. Reload Metadata settings to check the saved configuration.';
      } finally { this.refreshing = false; this.flushHealthRefresh(); }
    },
    async test(item) {
      if (this.busy || !this.canTest(item)) return;
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
      } finally { this.testing = null; this.flushHealthRefresh(); }
    },
  };
}
