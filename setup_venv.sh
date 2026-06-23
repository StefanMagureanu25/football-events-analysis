#!/usr/bin/env bash
#
# setup_venv.sh — setup COMPLET, dintr-o singură comandă:
#   1. instalează Java JDK 17 (necesar pentru Spark) dacă lipsește
#   2. instalează Python 3.10 dacă lipsește
#   3. creează venv-ul + instalează dependențele din requirements.txt
#   4. configurează JAVA_HOME automat la activarea venv-ului
#   5. înregistrează kernel-ul Jupyter și verifică totul
#
# Utilizare:
#   ./setup_venv.sh
#
# Suportă: macOS (Homebrew) și Linux (apt). Poate cere parola sudo pe Linux.

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

OS="$(uname -s)"
echo "================================================================"
echo " Setup COMPLET — Football Events Analysis   (OS: $OS)"
echo "================================================================"

# Pe macOS, încărcăm Homebrew în PATH-ul scriptului (shell-ul non-interactiv NU
# citește .zshrc, deci binarele brew — inclusiv python3.10 — pot lipsi din PATH).
if [[ "$OS" == "Darwin" ]]; then
  for b in /opt/homebrew/bin/brew /usr/local/bin/brew; do
    [[ -x "$b" ]] && eval "$("$b" shellenv)" && break
  done
fi

# =================================================================
# 1. JAVA JDK 17 (necesar pentru Apache Spark)
# =================================================================
ensure_java() {
  if java -version >/dev/null 2>&1; then
    echo "▶  Java deja prezent: $(java -version 2>&1 | head -1)"
    return 0
  fi
  echo "▶  Java lipsește — îl instalez ..."
  if [[ "$OS" == "Darwin" ]]; then
    ensure_brew
    brew install openjdk@17
  else
    sudo apt-get update -qq
    sudo apt-get install -y openjdk-17-jdk
  fi
}

# Detectează JAVA_HOME (după instalare sau dacă exista deja)
detect_java_home() {
  local jh=""
  if [[ "$OS" == "Darwin" ]]; then
    if command -v /usr/libexec/java_home >/dev/null 2>&1; then
      jh="$(/usr/libexec/java_home -v 17 2>/dev/null || /usr/libexec/java_home 2>/dev/null || true)"
    fi
    if [[ -z "$jh" ]] && command -v brew >/dev/null 2>&1; then
      local bp; bp="$(brew --prefix openjdk@17 2>/dev/null || true)"
      [[ -n "$bp" ]] && jh="$bp/libexec/openjdk.jdk/Contents/Home"
    fi
  else
    # Linux: caută în locațiile standard
    jh="$(dirname "$(dirname "$(readlink -f "$(command -v java)")")" 2>/dev/null || true)"
    [[ -d "/usr/lib/jvm/java-17-openjdk-amd64" ]] && jh="/usr/lib/jvm/java-17-openjdk-amd64"
    [[ -d "/usr/lib/jvm/java-17-openjdk-arm64" ]] && jh="/usr/lib/jvm/java-17-openjdk-arm64"
  fi
  echo "$jh"
}

# =================================================================
# 2. PYTHON 3.10
# =================================================================
ensure_python() {
  for cand in python3.10 python3.11 python3.12; do
    if command -v "$cand" >/dev/null 2>&1; then
      PYBIN="$cand"; return 0
    fi
  done
  echo "▶  Python 3.10 lipsește — îl instalez ..."
  if [[ "$OS" == "Darwin" ]]; then
    ensure_brew
    brew install python@3.10
    # Cale explicită — brew python@3.10 e keg-only, poate să nu fie pe PATH.
    local bp; bp="$(brew --prefix python@3.10 2>/dev/null || true)"
    if [[ -n "$bp" && -x "$bp/bin/python3.10" ]]; then
      PYBIN="$bp/bin/python3.10"
    else
      PYBIN="python3.10"
    fi
  else
    sudo apt-get update -qq
    sudo apt-get install -y python3.10 python3.10-venv python3.10-distutils
    PYBIN="python3.10"
  fi
}

# =================================================================
# Helper: Homebrew (doar macOS)
# =================================================================
ensure_brew() {
  if command -v brew >/dev/null 2>&1; then return 0; fi
  echo "▶  Homebrew lipsește — îl instalez (vei fi întrebat de confirmare/parolă) ..."
  /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
  # adaugă brew în PATH pentru sesiunea curentă
  if [[ -x /opt/homebrew/bin/brew ]]; then eval "$(/opt/homebrew/bin/brew shellenv)"; fi
  if [[ -x /usr/local/bin/brew ]];  then eval "$(/usr/local/bin/brew shellenv)";  fi
}

# =================================================================
# Execută pașii 1–2
# =================================================================
PYBIN=""
ensure_java
ensure_python
JAVA_HOME_DETECTED="$(detect_java_home)"
echo "▶  Folosesc Python: $PYBIN ($($PYBIN --version 2>&1))"
[[ -n "$JAVA_HOME_DETECTED" ]] && echo "▶  JAVA_HOME: $JAVA_HOME_DETECTED" || echo "⚠️  Nu am putut detecta JAVA_HOME automat."

# =================================================================
# 3. VENV + dependențe
# =================================================================
# Creează venv-ul și întoarce 0 doar dacă venv/bin/python a fost creat efectiv.
# Afișează eroarea reală (nu o înghite) — utilă la diagnoză.
create_venv() {
  rm -rf venv
  local out
  out="$("$PYBIN" -m venv venv 2>&1)"
  if [[ -x "$ROOT/venv/bin/python" ]]; then
    return 0
  fi
  [[ -n "$out" ]] && echo "$out" | sed 's/^/     /'
  return 1
}

if [[ -d "venv" && -x "$ROOT/venv/bin/python" ]]; then
  echo "▶  venv/ există deja și e valid — îl refolosesc."
else
  echo "▶  Creez venv/ cu $PYBIN ..."
  if ! create_venv; then
    # Cauza tipică pe Debian/Ubuntu: lipsește pachetul pythonX.Y-venv.
    PYVER="$("$PYBIN" -c 'import sys;print(f"{sys.version_info.major}.{sys.version_info.minor}")' 2>/dev/null || echo "3.10")"
    if [[ "$OS" != "Darwin" ]]; then
      echo "⚠️  Crearea a eșuat — instalez pachetul python${PYVER}-venv și reîncerc ..."
      sudo apt-get update -qq
      sudo apt-get install -y "python${PYVER}-venv" "python${PYVER}-distutils" 2>/dev/null \
        || sudo apt-get install -y "python${PYVER}-venv"
      if ! create_venv; then
        echo "❌ Crearea venv-ului tot a eșuat. Rulează manual și trimite-mi eroarea:" >&2
        echo "     $PYBIN -m venv venv" >&2
        exit 1
      fi
    else
      echo "❌ Crearea venv-ului a eșuat pe macOS. Încearcă:" >&2
      echo "     brew reinstall python@${PYVER}   (apoi rulează din nou ./setup_venv.sh)" >&2
      exit 1
    fi
  fi
fi

# Folosim Python-ul din venv prin cale ABSOLUTĂ — nu depindem de `python` din PATH.
VENV_PY="$ROOT/venv/bin/python"
echo "▶  venv OK: $("$VENV_PY" --version 2>&1)"

echo "▶  Actualizez pip ..."
"$VENV_PY" -m pip install --upgrade pip >/dev/null
echo "▶  Instalez dependențele (poate dura câteva minute) ..."
"$VENV_PY" -m pip install -r requirements.txt

# =================================================================
# 4. Persistă JAVA_HOME în activarea venv-ului
# =================================================================
if [[ -n "$JAVA_HOME_DETECTED" ]] && ! grep -q "JAVA_HOME=" venv/bin/activate; then
  {
    echo ""
    echo "# Setat automat de setup_venv.sh — necesar pentru Apache Spark"
    echo "export JAVA_HOME=\"$JAVA_HOME_DETECTED\""
  } >> venv/bin/activate
  export JAVA_HOME="$JAVA_HOME_DETECTED"
  echo "▶  JAVA_HOME adăugat în venv/bin/activate (se setează automat la activare)."
fi

# =================================================================
# 5. Kernel Jupyter + verificare
# =================================================================
echo "▶  Înregistrez kernel-ul Jupyter 'football-venv' ..."
"$VENV_PY" -m ipykernel install --user --name football-venv \
  --display-name "Python (football-venv)" >/dev/null

echo ""
echo "▶  Verificare importuri cheie ..."
"$VENV_PY" - <<'PY'
import sys
mods = ['pyspark','tensorflow','sklearn','numpy','pandas','pyarrow','matplotlib','seaborn','joblib']
ok = True
for m in mods:
    try:
        mod = __import__(m)
        print(f"  ✅ {m:<12} {getattr(mod,'__version__','?')}")
    except Exception as e:
        ok = False
        print(f"  ❌ {m:<12} EROARE: {e}")
sys.exit(0 if ok else 1)
PY
VERIFY=$?

echo ""
echo "================================================================"
if [[ $VERIFY -eq 0 ]]; then
  echo " ✅ Gata! Mediu configurat complet."
else
  echo " ⚠️  Setup terminat, dar unele importuri au eșuat (vezi mai sus)."
fi
echo "================================================================"
echo " Pași următori:"
echo "   source venv/bin/activate      # activează mediul (setează și JAVA_HOME)"
echo "   ./run_notebooks.sh            # rulează toate notebook-urile"
echo ""
echo " ⚠️  Descarcă dataset-ul de pe Kaggle în data/ (events.csv + ginf.csv)"
echo "     vezi livrabile/...-Set_date.txt"
echo "================================================================"
