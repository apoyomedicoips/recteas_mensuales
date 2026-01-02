// docs/auth.js

const AUTH_USERS = [
  {
    user: "dmeza",
    email: "dmeza.py@gmail.com",
    pass_sha256: "23b7f5fc18e11d24532077431a43f3996155fc9985e5dc6c8bd6d71801fbbf3e"
  }
];

async function sha256Hex(text) {
  const enc = new TextEncoder().encode(text);
  const buf = await crypto.subtle.digest("SHA-256", enc);
  const arr = Array.from(new Uint8Array(buf));
  return arr.map(b => b.toString(16).padStart(2, "0")).join("");
}

function isAuthed() {
  return sessionStorage.getItem(CONFIG.AUTH.SESSION_KEY) === "1";
}

function setAuthed(user) {
  sessionStorage.setItem(CONFIG.AUTH.SESSION_KEY, "1");
  sessionStorage.setItem(CONFIG.AUTH.USER_BADGE_KEY, user);
}

function clearAuth() {
  sessionStorage.removeItem(CONFIG.AUTH.SESSION_KEY);
  sessionStorage.removeItem(CONFIG.AUTH.USER_BADGE_KEY);
}

async function login(user, email, pass) {
  const u = (user || "").trim().toLowerCase();
  const e = (email || "").trim().toLowerCase();
  const p = pass || "";

  const passHash = await sha256Hex(p);

  const ok = AUTH_USERS.some(x =>
    x.user === u && x.email === e && x.pass_sha256 === passHash
  );

  if (ok) {
    setAuthed(u);
  }
  return ok;
}

