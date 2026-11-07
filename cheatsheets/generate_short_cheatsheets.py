#!/usr/bin/env python3
"""Generate one-page cheat sheets for published Build and Teach Shorts.

From the repository root:

    python3 -m pip install -r cheatsheets/requirements.txt
    python3 cheatsheets/generate_short_cheatsheets.py

Each Short gets exactly one US Letter page at ``shorts/<topic>/cheatsheet.pdf``.
Copy, palette, and logo match ``generate_cheatsheets.py``. These sheets are
the Short's definition, key points, and a tiny example. They are not the
Associate lesson summaries.

Future Shorts need a sheet too: add a record to ``SHORTS``, run this script,
commit the PDF, and add a video-to-PDF row on the root index.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.utils import ImageReader
from reportlab.platypus import (
    BaseDocTemplate,
    Flowable,
    Frame,
    PageTemplate,
    Paragraph,
    Spacer,
)

ROOT = Path(__file__).resolve().parents[1]


def _load_brand():
    path = Path(__file__).with_name("generate_cheatsheets.py")
    spec = importlib.util.spec_from_file_location("lesson_cheatsheets", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


brand = _load_brand()

PAGE_W, PAGE_H = brand.PAGE_W, brand.PAGE_H
HEADER_H = brand.HEADER_H
FOOTER_H = 36
LEFT, RIGHT = brand.LEFT, brand.RIGHT
CONTENT_W = brand.CONTENT_W
BLACK, ORANGE, OFFWHITE = brand.BLACK, brand.ORANGE, brand.OFFWHITE
MUTED, RULE = brand.MUTED, brand.RULE


def _esc(text: str) -> str:
    return brand._esc(text)


BODY = ParagraphStyle(
    "short_body",
    fontName="Inter",
    fontSize=10.5,
    leading=14.2,
    textColor=BLACK,
    spaceAfter=2,
)
REMEMBER = ParagraphStyle(
    "short_remember",
    fontName="Inter-Semibold",
    fontSize=11.5,
    leading=15,
    textColor=BLACK,
    spaceBefore=1,
    spaceAfter=2,
)


def _p(text: str) -> Paragraph:
    return Paragraph(_esc(text), BODY)


def _remember(text: str) -> Paragraph:
    return Paragraph(_esc(text), REMEMBER)


class Callout(Flowable):
    """Misconception check. Wraps text inside the frame."""

    def __init__(self, text: str):
        super().__init__()
        self.paragraph = Paragraph(_esc(text), BODY)
        self.pad_x = 10
        self.pad_y = 7

    def wrap(self, avail_width, avail_height):
        self.width = avail_width
        inner = avail_width - self.pad_x * 2 - 4
        _, height = self.paragraph.wrap(inner, avail_height)
        self._inner_height = height
        self.height = height + self.pad_y * 2
        return avail_width, self.height

    def draw(self):
        canvas = self.canv
        canvas.setFillColor(brand.ROW_ALT)
        canvas.setStrokeColor(RULE)
        canvas.setLineWidth(0.6)
        canvas.roundRect(0, 0, self.width, self.height, 3, fill=1, stroke=1)
        canvas.setFillColor(ORANGE)
        canvas.rect(0, 0, 3, self.height, fill=1, stroke=0)
        self.paragraph.drawOn(canvas, self.pad_x + 4, self.pad_y)


def _header_footer(canvas, doc, label: str, video_url: str):
    canvas.saveState()
    canvas.setFillColor(OFFWHITE)
    canvas.rect(0, 0, PAGE_W, PAGE_H, fill=1, stroke=0)
    canvas.setFillColor(BLACK)
    canvas.rect(0, PAGE_H - HEADER_H, PAGE_W, HEADER_H, fill=1, stroke=0)
    canvas.setFillColor(ORANGE)
    canvas.rect(0, PAGE_H - HEADER_H - 3, PAGE_W, 3, fill=1, stroke=0)
    canvas.drawImage(
        ImageReader(brand.LOGO),
        18,
        PAGE_H - HEADER_H + 8,
        width=42,
        height=42,
        mask="auto",
        preserveAspectRatio=True,
    )
    canvas.setFillColor(OFFWHITE)
    canvas.setFont("Inter-Bold", 12)
    canvas.drawString(70, PAGE_H - 28, "Build and Teach")
    canvas.setFillColor(ORANGE)
    canvas.setFont("Inter-Semibold", 8)
    canvas.drawString(70, PAGE_H - 42, "SHORT  ·  ONE-PAGE REFERENCE")
    canvas.setFillColor(OFFWHITE)
    canvas.setFont("Inter", 8.5)
    canvas.drawRightString(PAGE_W - 20, PAGE_H - 34, label)

    canvas.setStrokeColor(RULE)
    canvas.setLineWidth(0.4)
    canvas.line(LEFT, 30, PAGE_W - RIGHT, 30)
    canvas.setFillColor(MUTED)
    canvas.setFont("Inter", 7.5)
    canvas.drawString(LEFT, 16, video_url)
    canvas.restoreState()


def _story(sheet: dict) -> list:
    story: list = [
        Paragraph(_esc(sheet["kicker"]), brand.STYLES["kicker"]),
        Paragraph(_esc(sheet["title"]), brand.STYLES["title"]),
        Paragraph(_esc(sheet["definition"]), brand.STYLES["lede"]),
        brand._section("Key points"),
        brand._bullets(sheet["points"]),
        Spacer(1, 10),
        brand._section("Tiny example"),
        _p(sheet["example_note"]),
        Spacer(1, 6),
        brand.CodeBlock(sheet["code"], size=9, leading=12),
        Spacer(1, 10),
        brand._section("Misconception check"),
        Callout(sheet["misconception"]),
        Spacer(1, 12),
        brand._section("Remember"),
        _remember(sheet["remember"]),
    ]
    if sheet.get("free_note"):
        story.extend([Spacer(1, 8), _p(sheet["free_note"])])
    return story


def _build(path: Path, story: list, label: str, video_url: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    seen = {"n": 0}

    def on_page(canvas, doc):
        _header_footer(canvas, doc, label, video_url)
        seen["n"] = doc.page

    frame = Frame(
        LEFT,
        FOOTER_H + 4,
        CONTENT_W,
        PAGE_H - HEADER_H - 3 - FOOTER_H - 12,
        leftPadding=0,
        rightPadding=0,
        topPadding=0,
        bottomPadding=0,
        showBoundary=0,
    )
    document = BaseDocTemplate(
        str(path),
        pagesize=(PAGE_W, PAGE_H),
        title=label,
        author="Build and Teach",
        creator="cheatsheets/generate_short_cheatsheets.py",
        invariant=1,
    )
    document.addPageTemplates([PageTemplate(id="short", frames=[frame], onPage=on_page)])
    document.build(story)
    if seen["n"] != 1:
        raise RuntimeError(f"{path} is {seen['n']} pages; each Short sheet must be exactly 1")


def _check(path: Path, required: list[str]) -> None:
    from pypdf import PdfReader
    import pypdfium2 as pdfium

    reader = PdfReader(str(path))
    if len(reader.pages) != 1:
        raise RuntimeError(f"{path.name} has {len(reader.pages)} pages")
    text = reader.pages[0].extract_text() or ""
    missing = [phrase for phrase in required if phrase not in text]
    if missing:
        raise RuntimeError(f"{path.name} is missing extracted text: {missing}")
    document = pdfium.PdfDocument(str(path))
    page = document[0]
    scale = 2
    image = page.render(scale=scale).to_pil().convert("RGB")
    width, height = image.size
    pixels = image.load()
    offwhite = (246, 243, 238)
    header_px = int(61 * scale)
    footer_px = int(FOOTER_H * scale)
    edge_hits = 0
    for y in range(header_px + 2, height - footer_px - 2):
        for x in (0, 1, width - 2, width - 1):
            red, green, blue = pixels[x, y]
            if any(abs(channel - target) > 24 for channel, target in zip((red, green, blue), offwhite)):
                edge_hits += 1
    if edge_hits:
        raise RuntimeError(f"{path.name} has {edge_hits} non-background edge pixels")
    margin = int(22 * scale)
    last_ink = header_px
    for y in range(header_px, height - footer_px):
        if any(
            any(abs(channel - target) > 14 for channel, target in zip(pixels[x, y], offwhite))
            for x in range(margin, width - margin, 5)
        ):
            last_ink = y
    gap_pt = ((height - footer_px) - last_ink) / scale
    if gap_pt < 8:
        raise RuntimeError(f"{path.name} content is {gap_pt:.1f}pt from the footer")


# Wording follows the published Short scripts. Examples illustrate those
# scripts. They are not captured workspace output.
SHORTS = [
    {
        "slug": "lakehouse",
        "label": "Lakehouse",
        "video_url": "https://youtube.com/shorts/LB9BbsPiJX0",
        "kicker": "SHORT  ·  LAKEHOUSE",
        "title": "What is a lakehouse?",
        "definition": (
            "A lakehouse keeps data in open files in low-cost storage and adds a table layer, "
            "such as Delta Lake, so one copy can power SQL, data engineering, and machine learning."
        ),
        "points": [
            "A data lake is flexible storage for files such as CSV, JSON, and Parquet. Files alone are hard to trust as tables.",
            "A warehouse gives reliable tables for SQL and dashboards. It is usually more rigid, and data is often copied into it.",
            "A lakehouse combines the two: the files stay in lake storage, and the table layer adds reliability.",
            "In Databricks, one path is to save a DataFrame as a table, then query that table with SQL.",
        ],
        "example_note": "The Short's save-then-query shape. schema.table_name is a placeholder, not a recorded table.",
        "code": """
# save a DataFrame as a table, then query it
dataframe.write.saveAsTable("schema.table_name")
spark.sql("SELECT * FROM schema.table_name")
""",
        "misconception": (
            "A lakehouse is not just a folder of files, and it is not a second warehouse you must copy every table into. "
            "The files stay put. The table layer is what makes them reliable tables."
        ),
        "remember": "A lakehouse is lake storage with warehouse-style reliable tables.",
        "free_note": (
            "The Short says you can try saving a table and querying it on a free Databricks account. "
            "That does not mean every Databricks feature is included."
        ),
        "required": [
            "What is a lakehouse?",
            "Delta Lake",
            "saveAsTable",
            "LB9BbsPiJX0",
            "free Databricks account",
        ],
    },
    {
        "slug": "databricks",
        "label": "Databricks",
        "video_url": "https://youtube.com/shorts/P2MW-ws3pUQ",
        "kicker": "SHORT  ·  DATABRICKS",
        "title": "What is Databricks?",
        "definition": (
            "Databricks is one workspace for working with data. In that workspace you can engineer data, "
            "run analytics, and explore AI."
        ),
        "points": [
            "You write in notebooks, in Python or SQL.",
            "Apache Spark runs that code on compute.",
            "Results can be saved as reliable Delta tables.",
            "You can then query those tables with SQL and build dashboards.",
        ],
        "example_note": "The Short's sales example, as a sequence. It is not a measured pipeline result.",
        "code": """
# 1. Raw sales files land in storage.
# 2. A notebook reads them with Spark and cleans them.
# 3. Save a Delta table.
# 4. Query that table in the same workspace.
""",
        "misconception": (
            "Databricks is not only a notebook, and not only a place to drop files. "
            "The notebook, Spark compute, and tables are parts of one workspace."
        ),
        "remember": "Databricks is the home base where your data work actually happens.",
        "free_note": (
            "The Short says this read, clean, save, and query example can be done on a free account. "
            "It does not say every analytics or AI feature is free."
        ),
        "required": [
            "What is Databricks?",
            "Apache Spark",
            "Delta",
            "P2MW-ws3pUQ",
            "free account",
        ],
    },
    {
        "slug": "delta-lake",
        "label": "Delta Lake",
        "video_url": "https://youtube.com/shorts/Wh7SJFTdJWs",
        "kicker": "SHORT  ·  DELTA LAKE",
        "title": "What is Delta Lake?",
        "definition": (
            "Delta Lake stores table data as Parquet files and adds a transaction log that records every change."
        ),
        "points": [
            "Transactions: a write finishes completely, or not at all.",
            "Schema rules protect columns from bad data.",
            "Time travel reads an older version of the same table.",
            "DESCRIBE HISTORY lists versions. VERSION AS OF reads an earlier one.",
        ],
        "example_note": "Use the earlier version number from history. The Short does not fix that number.",
        "code": """
DESCRIBE HISTORY schema.table_name;

SELECT *
FROM schema.table_name
VERSION AS OF <earlier_version>
""",
        "misconception": (
            "A folder of Parquet files is not, by itself, a Delta table. "
            "Parquet stores the data. The transaction log makes the table trustworthy."
        ),
        "remember": "Parquet stores the data. Delta Lake makes the table trustworthy.",
        "free_note": (
            "The Short says DESCRIBE HISTORY and VERSION AS OF work on a free account. "
            "This sheet does not add other Delta features to that claim."
        ),
        "required": [
            "What is Delta Lake?",
            "DESCRIBE HISTORY",
            "VERSION AS OF",
            "Wh7SJFTdJWs",
            "free account",
        ],
    },
    {
        "slug": "dataframe",
        "label": "DataFrame",
        "video_url": "https://youtube.com/shorts/qQPjfrxBsRI",
        "kicker": "SHORT  ·  DATAFRAME",
        "title": "What is a Spark DataFrame?",
        "definition": "A Spark DataFrame organizes data into rows and named columns.",
        "points": [
            "Picture a customer list: a name column, a city column, and one row per customer.",
            "Create it in a Databricks notebook with Python. A variable such as customers refers to that DataFrame.",
            "select describes a transformation. show asks Spark to produce a result.",
            "A DataFrame is not automatically a saved table. Save it separately if you need to keep the data.",
        ],
        "example_note": "Two customer rows, then select and show. Angle brackets are placeholders. No live printout is recorded here.",
        "code": """
customers = spark.createDataFrame(
    [("<name>", "<city>"), ("<name>", "<city>")],
    ["name", "city"],
)
customers.select("name").show()
""",
        "misconception": (
            "select does not by itself print or save anything. show produces a result. "
            "The customers variable is still not a saved table."
        ),
        "remember": "A DataFrame is a way to work with rows and columns using code.",
        "free_note": "The Short says you can try this tiny example in Databricks Free Edition.",
        "required": [
            "What is a Spark DataFrame?",
            "createDataFrame",
            ".show()",
            "qQPjfrxBsRI",
            "Free Edition",
        ],
    },
    {
        "slug": "notebook",
        "label": "Notebook",
        "video_url": "https://youtube.com/shorts/JlCLoWxcSg8",
        "kicker": "SHORT  ·  NOTEBOOK",
        "title": "What is a Databricks notebook?",
        "definition": (
            "A Databricks notebook keeps code, explanations, and results together. "
            "Work is organized into cells instead of one long script."
        ),
        "points": [
            "A code cell holds instructions, such as Python. Run it on compute and the result appears below the cell.",
            "A text cell explains what the code is doing.",
            "A later cell can use a variable from an earlier cell. Run the earlier cell first, or the variable may not exist yet.",
            "Saving the notebook keeps the code. It does not keep variables into a new session. Run from the top for a repeatable lesson.",
        ],
        "example_note": "Illustration of the Short: one cell stores a number, the next adds one. This is not a captured output.",
        "code": """
# code cell 1
value = 1

# code cell 2 — run cell 1 first in this session
print(value + 1)
""",
        "misconception": (
            "Saving a notebook does not save its variables. "
            "A new session still needs the cells run in order from the top."
        ),
        "remember": "A notebook brings code and its results into one place.",
        "free_note": "The Short says you can practice this in Databricks Free Edition.",
        "required": [
            "What is a Databricks notebook?",
            "value = 1",
            "print(value + 1)",
            "JlCLoWxcSg8",
            "Free Edition",
        ],
    },
]


def main() -> None:
    if not (ROOT / "brand" / "build-and-teach-gear.png").is_file():
        raise SystemExit("Missing brand/build-and-teach-gear.png")
    for sheet in SHORTS:
        path = ROOT / "shorts" / sheet["slug"] / "cheatsheet.pdf"
        _build(path, _story(sheet), sheet["label"], sheet["video_url"])
        _check(path, sheet["required"])
        print(f"wrote {path.relative_to(ROOT)} (1 page)")
    print(f"sheets: {len(SHORTS)}")


if __name__ == "__main__":
    main()
