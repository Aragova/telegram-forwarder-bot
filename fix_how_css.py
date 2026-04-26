from pathlib import Path

path = Path("landing/styles.css")
text = path.read_text()

marker = "/* === FINAL: how it works premium icons === */"

if marker in text:
    text = text[:text.index(marker)].rstrip()

new_css = """

/* === HOW IT WORKS — FINAL SCOPED === */

.how-section {
  padding: 76px 24px;
  background: #f5f9ff;
}

.how-container {
  width: min(1160px, calc(100% - 48px));
  margin: 0 auto;
}

.how-head {
  max-width: 760px;
  margin: 0 auto 34px;
  text-align: center;
}

.how-grid {
  display: grid;
  grid-template-columns: 1fr 390px;
  gap: 22px;
}

.how-section .steps {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 16px;
}

.how-section .step-card {
  min-height: 170px;
  padding: 24px;
  border-radius: 24px;
  background: #fff;
  border: 1px solid #e7ebf5;
  box-shadow: 0 16px 38px rgba(35,44,88,.06);
  display: flex;
  flex-direction: column;
  justify-content: space-between;
}

.how-section .step-card:nth-child(5) {
  grid-column: span 2;
  min-height: 138px;
}

.how-section .step-card::after {
  display: none !important;
}

.how-section .step-num {
  width: 46px;
  height: 46px;
  border-radius: 15px;
  display: grid;
  place-items: center;
  background: linear-gradient(135deg,#3f75ff,#7453f6);
  color: #fff;
  font-weight: 800;
  margin-bottom: 24px;
}

.how-section .step-card h3 {
  margin: 0 0 7px !important;
  font-size: 19px;
  line-height: 1.18;
}

.how-section .step-card p {
  margin: 0;
  color: #59688f;
  font-size: 14px;
  line-height: 1.45;
  font-weight: 600;
}

.how-section .step-chip {
  display: none;
}

.how-section .demo-card {
  padding: 28px;
  border-radius: 28px;
  background:
    radial-gradient(circle at 86% 8%, rgba(57,223,247,.22), transparent 32%),
    linear-gradient(180deg,#121936,#0b1025);
  color: #fff;
  box-shadow: 0 24px 60px rgba(18,25,54,.22);
}

.how-section .demo-top {
  display: flex;
  align-items: center;
  gap: 13px;
  margin-bottom: 22px;
}

.how-section .demo-logo {
  width: 48px;
  height: 48px;
  border-radius: 16px;
  display: grid;
  place-items: center;
  background: linear-gradient(135deg,#39dff7,#3766ff);
  color: #fff;
  font-weight: 800;
}

.how-section .demo-message {
  padding: 16px;
  border-radius: 18px;
  background: rgba(255,255,255,.08);
  border: 1px solid rgba(255,255,255,.08);
  margin-bottom: 12px;
}

.how-section .demo-status {
  margin-top: 20px;
  padding: 16px;
  border-radius: 18px;
  background: #fff;
  color: #121936;
}

.how-section .demo-status div {
  display: flex;
  justify-content: space-between;
  gap: 18px;
  padding: 7px 0;
}

@media (max-width: 980px) {
  .how-grid,
  .how-section .steps {
    grid-template-columns: 1fr;
  }

  .how-section .step-card:nth-child(5) {
    grid-column: auto;
  }
}
"""

path.write_text(text + new_css)
