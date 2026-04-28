(function () {
  function registerServiceWorker() {
    if ('serviceWorker' in navigator) {
      navigator.serviceWorker.register('/service-worker.js').catch(function () {});
    }
  }

  function showLoading() {
    var overlay = document.getElementById('page-loading-overlay');
    if (!overlay) return;
    overlay.classList.add('is-visible');
  }

  window.addEventListener('load', function () {
    registerServiceWorker();
    var overlay = document.getElementById('page-loading-overlay');
    if (overlay) overlay.classList.remove('is-visible');
  });

  document.addEventListener('click', function (event) {
    var link = event.target.closest('a');
    if (!link) return;
    if (link.target === '_blank' || link.hasAttribute('download')) return;
    var href = link.getAttribute('href') || '';
    if (!href || href.startsWith('#') || href.startsWith('javascript:')) return;
    showLoading();
  }, true);

  document.addEventListener('submit', function () {
    showLoading();
  }, true);
})();
