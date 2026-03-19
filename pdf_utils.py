from pathlib import Path
from typing import Iterable, List

from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas


def make_simple_error_pdf(out_path: Path, title: str, lines: List[str]) -> None:
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    c = canvas.Canvas(str(out_path), pagesize=A4)
    width, height = A4

    y = height - 72
    c.setFont("Helvetica-Bold", 16)
    c.drawString(72, y, title)

    y -= 28
    c.setFont("Helvetica", 11)
    for line in lines:
        for wrapped in _wrap(line, 95):
            c.drawString(72, y, wrapped)
            y -= 16
            if y < 72:
                c.showPage()
                y = height - 72
                c.setFont("Helvetica", 11)

    c.showPage()
    c.save()


def _wrap(text: str, max_chars: int) -> Iterable[str]:
    if len(text) <= max_chars:
        return [text]
    words = text.split()
    out = []
    cur = ""
    for w in words:
        if not cur:
            cur = w
        elif len(cur) + 1 + len(w) <= max_chars:
            cur += " " + w
        else:
            out.append(cur)
            cur = w
    if cur:
        out.append(cur)
    return out
