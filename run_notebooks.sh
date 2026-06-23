#!/usr/bin/env bash
#
# run_notebooks.sh — rulează toate notebook-urile proiectului în ordinea corectă
# de dependențe, salvând output-urile în fișiere (--inplace).
#
# Utilizare:
#   ./run_notebooks.sh                # rulează notebook-urile din notebooks/
#   ./run_notebooks.sh livrabile      # rulează altă mapă (ex: livrabile/)
#   FOLDER=livrabile ./run_notebooks.sh
#   TIMEOUT=3600 ./run_notebooks.sh   # mărește timeout-ul per notebook (secunde)
#
# Ordinea de rulare respectă dependențele dintre artefacte:
#   Cerința 4 scrie data/processed_shots/ + models/rf_pipeline_model/  (folosite de 5 și 7)
#   Cerința 6 scrie models/tf_goal_predictor.keras + tf_scaler.joblib

set -uo pipefail

# --- Rădăcina proiectului = directorul în care se află scriptul -----------------
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

FOLDER="${1:-${FOLDER:-notebooks}}"
TIMEOUT="${TIMEOUT:-1800}"
PREFIX="505_Magureanu_Stefan_Ionut"

# Ordinea: intro (independent) + 2 → 4 → 3 → 5 → 6 → 7
ORDER=(
  "Proiect_BigData"
  "Cod_sursa_cerinta_2"
  "Cod_sursa_cerinta_4"
  "Cod_sursa_cerinta_3"
  "Cod_sursa_cerinta_5"
  "Cod_sursa_cerinta_6"
  "Cod_sursa_cerinta_7"
)

# --- Verificări preliminare ------------------------------------------------------
if [[ ! -d "venv" ]]; then
  echo "❌ Nu găsesc venv/. Creează-l și instalează dependențele întâi." >&2
  exit 1
fi
if [[ ! -f "data/events.csv" ]]; then
  echo "⚠️  data/events.csv lipsește — descarcă dataset-ul de pe Kaggle (vezi Set_date.txt)." >&2
  echo "    Notebook-urile care citesc CSV-ul vor eșua fără el." >&2
fi
if [[ ! -d "$FOLDER" ]]; then
  echo "❌ Mapa '$FOLDER' nu există." >&2
  exit 1
fi

# Sursăm activate pentru JAVA_HOME (necesar Spark), dar apelăm Python prin cale
# ABSOLUTĂ — nu depindem de `jupyter`/`python` din PATH (pe unele sisteme lipsesc).
# shellcheck disable=SC1091
source venv/bin/activate
VENV_PY="$ROOT/venv/bin/python"
if [[ ! -x "$VENV_PY" ]]; then
  echo "❌ venv/bin/python lipsește. Rulează întâi ./setup_venv.sh" >&2
  exit 1
fi

echo "================================================================"
echo " Rulez notebook-urile din: $FOLDER/"
echo " Timeout per notebook: ${TIMEOUT}s"
echo "================================================================"

declare -a RESULTS=()
FAILED=0

for name in "${ORDER[@]}"; do
  nb="$FOLDER/${PREFIX}-${name}.ipynb"
  if [[ ! -f "$nb" ]]; then
    echo "⚠️  Sar peste (lipsește): $nb"
    RESULTS+=("SKIP  $name")
    continue
  fi

  echo ""
  echo "▶  $name ..."
  t0=$(date +%s)
  if "$VENV_PY" -m nbconvert --to notebook --execute --inplace \
       --ExecutePreprocessor.timeout="$TIMEOUT" "$nb" > /tmp/nb_run.log 2>&1; then
    t1=$(date +%s)
    echo "✅ $name — OK ($((t1 - t0))s)"
    RESULTS+=("OK    $name ($((t1 - t0))s)")
  else
    t1=$(date +%s)
    echo "❌ $name — EROARE ($((t1 - t0))s). Ultimele linii din log:"
    tail -n 15 /tmp/nb_run.log | sed 's/^/     /'
    RESULTS+=("FAIL  $name ($((t1 - t0))s)")
    FAILED=$((FAILED + 1))
  fi
done

# --- Sumar -----------------------------------------------------------------------
echo ""
echo "================================================================"
echo " SUMAR"
echo "================================================================"
for r in "${RESULTS[@]}"; do echo "  $r"; done
echo "----------------------------------------------------------------"
if [[ "$FAILED" -eq 0 ]]; then
  echo "🎉 Toate notebook-urile au rulat fără erori."
  exit 0
else
  echo "⚠️  $FAILED notebook(uri) au eșuat — vezi log-urile de mai sus."
  exit 1
fi
