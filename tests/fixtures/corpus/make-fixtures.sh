#!/usr/bin/env bash
# Provenance of the corpus fixtures (tests/e2e.rs). Run by hand from this directory; the outputs
# are committed and CI never runs this script.
#
# Tools (as used on 2026-10-16, macOS): pandoc 3.x, pdflatex (TeX Live), poppler `pdftoppm`,
# macOS `say`, ffmpeg. img2pdf + pypdf come from a throwaway uv environment (`uv run --with`),
# nothing is installed system-wide.
#
#   chart.png / icon.png   make-figures.py (pillow): a ~19 KB chart on page 2 (a real figure,
#                     kept) and a 138-byte icon on page 1 (below the 8 KiB threshold, dropped)
#   born_digital.pdf  pandoc + pdflatex, 3 pages (raw `\newpage` blocks in source.md)
#   tables.docx / book.epub (--split-level=2: one chapter per section) / slides.pptx /
#   page.html (--embed-resources: images as data URIs) / document.odt   pandoc from source.md
#   scanned.pdf       every page of born_digital.pdf rendered at 150 dpi (grey PNG) → img2pdf:
#                     no text layer at all
#   mixed.pdf         pages 1 and 3 of born_digital.pdf + page 2 of scanned.pdf (pypdf)
#   alternating.pdf   pages 1 and 3 of scanned.pdf + page 2 of born_digital.pdf (pypdf): two
#                     separate OCR batches (p0-0, p2-2) for the retry test
#   text.png          page 1 of born_digital.pdf rendered at 100 dpi, grey
#   speech.wav        `say` of the fixed paragraph below → ffmpeg 16 kHz mono s16le
set -euo pipefail
cd "$(dirname "$0")"

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

uv run --no-project --with pillow==11.3.0 python make-figures.py   # chart.png, icon.png

pandoc source.md -s -o born_digital.pdf --pdf-engine=pdflatex -V geometry:margin=2.5cm -V fontsize=11pt
pandoc source.md -s -o tables.docx
pandoc source.md -s -o book.epub --split-level=2
pandoc source.md -s -o slides.pptx
pandoc source.md -s --embed-resources -o page.html
pandoc source.md -s -o document.odt

# Lossless (PNG → Flate): docling-serve-cpu v1.37 OCR (easyocr) of the same pages as JPEG
# (grey q70 or RGB q85, 150 or 300 dpi) recovered only ~30 % of the text; see the task-11 report.
pdftoppm -r 150 -gray -png born_digital.pdf "$tmp/scan"
uv run --no-project --with img2pdf==0.6.1 img2pdf "$tmp"/scan-*.png -o scanned.pdf

uv run --no-project --with pypdf==6.1.1 python - <<'PY'
from pypdf import PdfReader, PdfWriter

born = PdfReader("born_digital.pdf")
scanned = PdfReader("scanned.pdf")
assert len(born.pages) == 3 and len(scanned.pages) == 3, (len(born.pages), len(scanned.pages))
w = PdfWriter()
w.add_page(born.pages[0])
w.add_page(scanned.pages[1])
w.add_page(born.pages[2])
with open("mixed.pdf", "wb") as f:
    w.write(f)
w = PdfWriter()
w.add_page(scanned.pages[0])
w.add_page(born.pages[1])
w.add_page(scanned.pages[2])
with open("alternating.pdf", "wb") as f:
    w.write(f)
PY

pdftoppm -r 100 -gray -png -f 1 -l 1 -singlefile born_digital.pdf text

cat > "$tmp/speech.txt" <<'TXT'
Good morning and welcome to the Quillmere harbour report. This morning the tide turned at
six forty, and the pilot boat brought the grain ship Hollowmere safely through the channel.
The dredger has finished its work off Dunlin Spit, and the soundings now show more than six
metres of water at the lowest tide. The harbour master thanks the crew of the dredger for
working through two nights of rain. On the northern breakwater, masons have set the new
granite block beside the beacon, and the cracked blocks near the elbow will be checked again
in the spring. The fishing fleet landed a good catch of mackerel and crab this week, and the
market hall stayed open late on Thursday to clear the boxes before the weekend. Visitors are
reminded that the yacht moorings in the inner basin are being renewed, and that any boat
still on a swing mooring should move to the visitors pontoon by Friday evening. At the
Saltreach lighthouse the keeper reports that the clockwork is running well, and the foghorn
compressor passed its pressure test on Tuesday. The weather for the coming days looks
settled, with light winds from the west and good visibility, though a sea mist may form at
dawn. Lastly, the harbour office will close early on Saturday for the annual lifeboat
festival. There will be a parade of boats at noon, a rope race on the slipway, and fresh
pasties at the lifeboat station all afternoon. Everyone is welcome, and every coin raised
goes to the crew who keep this coast safe. That is the end of this week's harbour report.
TXT
say -r 160 -f "$tmp/speech.txt" -o "$tmp/speech.aiff"
ffmpeg -nostdin -loglevel error -y -i "$tmp/speech.aiff" -ac 1 -ar 16000 -c:a pcm_s16le speech.wav

ls -l born_digital.pdf scanned.pdf mixed.pdf alternating.pdf tables.docx book.epub slides.pptx page.html document.odt text.png speech.wav
