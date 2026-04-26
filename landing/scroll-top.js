(function () {
  function initScrollTopButton() {
    let btn = document.getElementById('scrollTopBtn');

    if (!btn) {
      btn = document.createElement('button');
      btn.id = 'scrollTopBtn';
      btn.className = 'scroll-top-btn';
      btn.setAttribute('aria-label', 'Наверх');
      btn.innerHTML = `
        <svg viewBox="0 0 24 24" aria-hidden="true">
          <path d="M12 5l-7 7m7-7l7 7M12 5v14"></path>
        </svg>
      `;
      document.body.appendChild(btn);
    }

    window.addEventListener('scroll', () => {
      if (window.scrollY > 400) {
        btn.classList.add('show');
      } else {
        btn.classList.remove('show');
      }
    });

    btn.addEventListener('click', () => {
      window.scrollTo({ top: 0, behavior: 'smooth' });
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initScrollTopButton);
  } else {
    initScrollTopButton();
  }
})();
