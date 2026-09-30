/* Explicit provider linking; searches and previews never add a library series. */
function seriesMetadataLinks(seriesId) {
  var endpoint = "/api/v1/series/" + seriesId + "/metadata-links";
  return {
    open: false, loading: true, busy: false, error: "", message: "", trigger: null,
    current: {}, identities: [], sources: [], source: "", query: "",
    results: [], candidate: null, searched: false, offset: 0, nextOffset: null,
    generation: 0, disposed: false, linking: false,
    init: function () { this.load(); },
    destroy: function () { this.disposed = true; this.generation += 1; },
    request: async function (url, body) {
      var response = await fetch(url, body === undefined ? { cache: "no-store" } : {
        method: "POST", headers: { "Content-Type": "application/json", "X-CSRF-Token": readCsrfTokenFromBody() },
        body: JSON.stringify(body),
      });
      var data = await response.json();
      if (!response.ok) throw new Error(_extractApiErrorMessage(response, data, "Could not load provider metadata. Try again."));
      return data;
    },
    load: async function () {
      try {
        var data = await this.request(endpoint);
        if (this.disposed) return;
        this.current = data.current;
        this.identities = data.identities;
        this.sources = data.sources;
        this.source = this.sources.length ? this.sources[0].source : "";
        this.query = this.current.title;
        this.error = "";
      } catch (error) { this.error = error.message; }
      finally { this.loading = false; }
    },
    label: function (namespace) { return { comicvine: "ComicVine", metron: "Metron", gcd: "GCD", locg: "LOCG" }[namespace] || namespace; },
    sourceOptions: function () { return this.sources.map(item => ({ value: item.source, label: item.label })); },
    describe: function (series) {
      return [series.year_start || "Year unknown", series.publisher || "Publisher unknown", series.issue_count === null ? "Issue count unknown" : series.issue_count + " issues"].join(" · ");
    },
    openLink: function () {
      this.trigger = document.activeElement;
      this.open = true;
      this.error = "";
      this.$nextTick(() => (this.sources.length && !this.candidate ? this.$refs.linkQuery : this.$refs.linkDialog).focus({ preventScroll: true }));
    },
    closeLink: function () {
      this.open = false;
      this.generation += 1;
      if (!this.linking) this.busy = false;
      if (this.trigger && this.trigger.isConnected) this.trigger.focus({ preventScroll: true });
    },
    resetSearch: function () {
      this.generation += 1;
      this.busy = false;
      this.results = [];
      this.candidate = null;
      this.searched = false;
      this.nextOffset = null;
      this.offset = 0;
      this.error = "";
    },
    search: async function (offset) {
      if (this.busy || !this.source || !this.query.trim()) return;
      var generation = ++this.generation;
      this.busy = true;
      this.error = "";
      try {
        var data = await this.request("/api/v1/metadata/search", {
          query: this.query.trim(), sources: [this.source], limit_per_source: 10,
          offsets: { [this.source]: offset || 0 }, search_mode: "full",
        });
        if (this.disposed || generation !== this.generation) return;
        var outcome = data.sources.find(item => item.source === this.source);
        if (!outcome || (outcome.status !== "ok" && outcome.status !== "empty")) throw new Error("This provider is unavailable. Check Metadata settings or try again later.");
        this.results = data.results;
        this.offset = offset || 0;
        this.nextOffset = outcome.next_offset;
        this.candidate = null;
        this.searched = true;
      } catch (error) { if (generation === this.generation) this.error = error.message; }
      finally { if (generation === this.generation) this.busy = false; }
    },
    preview: async function (result) {
      if (this.busy) return;
      var generation = ++this.generation;
      this.busy = true;
      this.error = "";
      try {
        var data = await this.request(endpoint + "/preview", { source: result.source, external_id: result.external_id });
        if (this.disposed || generation !== this.generation) return;
        this.candidate = data;
        this.$nextTick(() => requestAnimationFrame(() => {
          if (this.open && this.candidate) this.$refs.reviewHeading.focus({ preventScroll: true });
        }));
      } catch (error) { if (generation === this.generation) this.error = error.message; }
      finally { if (generation === this.generation) this.busy = false; }
    },
    conflict: function () {
      if (!this.candidate) return "";
      var review = this.candidate.review;
      if (review.owner_local_id && review.owner_local_id !== seriesId) return "This provider record is already linked to another library series. No series will be merged.";
      if (review.current_external_id && review.current_external_id !== review.external_id) return "This series already has a different match for this provider. No match will be replaced.";
      return "";
    },
    confirm: async function () {
      if (this.busy || !this.candidate || this.conflict()) return;
      this.busy = true;
      this.error = "";
      var candidate = this.candidate;
      this.linking = true;
      try {
        await this.request(endpoint + "/confirm", {
          event_id: candidate.review.event_id, fingerprint: candidate.review.fingerprint,
          review_revision: candidate.review.review_revision, source_revision: candidate.source_revision,
        });
        if (this.disposed) return;
        await this.load();
        this.candidate = null;
        this.results = [];
        this.searched = false;
        this.message = "Provider linked. Use Refresh metadata to fill metadata gaps. Files and issue matches are unchanged.";
        this.closeLink();
      } catch (error) { if (!this.disposed) this.error = error.message; }
      finally { this.busy = false; this.linking = false; }
    },
    trapFocus: function (event) {
      if (!this.open) return;
      var elements = Array.from(this.$refs.linkDialog.querySelectorAll('button:not([disabled]), input:not([disabled]), a[href], [tabindex="0"]')).filter(el => el.getClientRects().length > 0);
      var first = elements[0], last = elements[elements.length - 1];
      if (!first) { event.preventDefault(); return; }
      if (event.shiftKey && (document.activeElement === first || document.activeElement === this.$refs.reviewHeading || document.activeElement === this.$refs.linkDialog)) {
        event.preventDefault(); last.focus();
      } else if (!event.shiftKey && (document.activeElement === last || document.activeElement === this.$refs.linkDialog)) { event.preventDefault(); first.focus(); }
    },
  };
}
