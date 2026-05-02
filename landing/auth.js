const authDict = {
  ru: {
    auth_title: 'Войти в ViMi',
    auth_subtitle: 'Управляйте автопостингом, очередями и реакциями Telegram-каналов из одного кабинета.',
    auth_signin_telegram: 'Войти через Telegram',
    auth_back_home: 'Вернуться на главную',
    auth_widget_placeholder: 'Здесь будет подключен Telegram Login Widget'
  },
  en: {
    auth_title: 'Sign in to ViMi',
    auth_subtitle: 'Manage Telegram autoposting, queues, and reactions from one dashboard.',
    auth_signin_telegram: 'Sign in with Telegram',
    auth_back_home: 'Back to home',
    auth_widget_placeholder: 'Telegram Login Widget will be connected here'
  }
};

const authCurrentLang = document.getElementById('currentLang');
const authToggle = document.getElementById('langToggle');

function setAuthLanguage(lang) {
  const pack = authDict[lang] || authDict.ru;
  document.documentElement.lang = lang;
  document.documentElement.dataset.lang = lang;
  authCurrentLang.textContent = lang.toUpperCase();

  document.querySelectorAll('[data-i18n]').forEach((el) => {
    const value = pack[el.dataset.i18n];
    if (value) {
      el.textContent = value;
    }
  });

  localStorage.setItem('vimi_lang', lang);
}

authToggle.addEventListener('click', () => {
  const next = (localStorage.getItem('vimi_lang') || 'ru') === 'ru' ? 'en' : 'ru';
  setAuthLanguage(next);
});

setAuthLanguage(localStorage.getItem('vimi_lang') || 'ru');
