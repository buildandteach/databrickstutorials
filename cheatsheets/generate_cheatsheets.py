#!/usr/bin/env python3
"""Generate the Build and Teach lesson cheat-sheet PDFs.

From the repository root:

    python3 -m pip install -r cheatsheets/requirements.txt
    python3 cheatsheets/generate_cheatsheets.py

Each Associate lesson gets one compact PDF at
``<lesson>/cheatsheet.pdf`` (one or two US Letter pages). Future lessons
need a matching sheet: add a builder below, run this script, commit the
PDF, and link it from the lesson README and the root lesson index.

The sheets are quick-reference snippets and check values taken from the
lesson notebooks. They are not full scripts.

Published Shorts use ``cheatsheets/generate_short_cheatsheets.py`` instead.
Each Short gets its own one-page PDF. Do not fold a Short into these lesson sheets.
"""

from __future__ import annotations

from pathlib import Path

from PIL import Image
from reportlab.lib.colors import Color, white
from reportlab.lib.pagesizes import letter
from reportlab.lib.utils import ImageReader
from reportlab.lib.styles import ParagraphStyle
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    BaseDocTemplate,
    CondPageBreak,
    Flowable,
    Frame,
    KeepTogether,
    ListFlowable,
    ListItem,
    PageTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
)

ROOT = Path(__file__).resolve().parents[1]
LOGO_PATH = ROOT / "brand" / "build-and-teach-gear.png"
FONT_DIR = Path("/usr/share/fonts/truetype")

BLACK = Color(11 / 255, 11 / 255, 11 / 255)
ORANGE = Color(1.0, 89 / 255, 45 / 255)
OFFWHITE = Color(246 / 255, 243 / 255, 238 / 255)
MUTED = Color(58 / 255, 53 / 255, 50 / 255)
RULE = Color(226 / 255, 218 / 255, 208 / 255)
CODE_BG = Color(1.0, 252 / 255, 248 / 255)
ROW_ALT = Color(255 / 255, 247 / 255, 242 / 255)

PAGE_W, PAGE_H = letter
HEADER_H = 58
FOOTER_H = 32
LEFT = 36
RIGHT = 36
CONTENT_W = PAGE_W - LEFT - RIGHT

pdfmetrics.registerFont(TTFont("Inter", str(FONT_DIR / "macos/Inter-Regular.ttf")))
pdfmetrics.registerFont(TTFont("Inter-Bold", str(FONT_DIR / "macos/Inter-Bold.ttf")))
pdfmetrics.registerFont(TTFont("Inter-Semibold", str(FONT_DIR / "macos/Inter-SemiBold.ttf")))
pdfmetrics.registerFont(TTFont("JB", str(FONT_DIR / "jetbrains-mono/JetBrainsMono-Regular.ttf")))
pdfmetrics.registerFont(TTFont("JB-Bold", str(FONT_DIR / "jetbrains-mono/JetBrainsMono-Bold.ttf")))


def _prepared_logo() -> Image.Image:
    """Drop the logo's near-black field so it sits on the #0b0b0b header."""
    image = Image.open(LOGO_PATH).convert("RGBA")
    pixels = image.load()
    width, height = image.size
    for y in range(height):
        for x in range(width):
            red, green, blue, _alpha = pixels[x, y]
            if red < 22 and green < 22 and blue < 22:
                pixels[x, y] = (11, 11, 11, 0)
    return image


LOGO = _prepared_logo()


def _esc(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


STYLES = {
    "kicker": ParagraphStyle(
        "kicker",
        fontName="Inter-Semibold",
        fontSize=8.5,
        leading=11,
        textColor=ORANGE,
        tracking=0.4,
        spaceAfter=1,
    ),
    "title": ParagraphStyle(
        "title",
        fontName="Inter-Bold",
        fontSize=18,
        leading=21,
        textColor=BLACK,
        spaceAfter=3,
    ),
    "lede": ParagraphStyle(
        "lede",
        fontName="Inter",
        fontSize=9.5,
        leading=12.5,
        textColor=MUTED,
        spaceAfter=6,
    ),
    "section": ParagraphStyle(
        "section",
        fontName="Inter-Bold",
        fontSize=11,
        leading=14,
        textColor=BLACK,
        spaceBefore=2,
        spaceAfter=2,
    ),
    "body": ParagraphStyle(
        "body",
        fontName="Inter",
        fontSize=9.5,
        leading=12.4,
        textColor=BLACK,
        spaceAfter=2,
    ),
    "bullet": ParagraphStyle(
        "bullet",
        fontName="Inter",
        fontSize=9.5,
        leading=12.2,
        textColor=BLACK,
        leftIndent=0,
        bulletIndent=0,
    ),
    "footer": ParagraphStyle(
        "footer",
        fontName="Inter",
        fontSize=8,
        leading=10,
        textColor=MUTED,
    ),
    "th": ParagraphStyle(
        "th",
        fontName="Inter-Semibold",
        fontSize=8,
        leading=10,
        textColor=white,
    ),
    "td": ParagraphStyle(
        "td",
        fontName="Inter",
        fontSize=8,
        leading=10,
        textColor=BLACK,
    ),
    "tdm": ParagraphStyle(
        "tdm",
        fontName="JB",
        fontSize=7.5,
        leading=9.5,
        textColor=BLACK,
    ),
}


class CodeBlock(Flowable):
    """Monospace snippet. Raises if any line would paint past the frame."""

    def __init__(self, source: str, size: float = 8.0, leading: float = 10.4):
        super().__init__()
        self.lines = source.strip("\n").split("\n")
        self.size = size
        self.leading = leading
        self.pad_x = 8
        self.pad_y = 6
        self._width = CONTENT_W

    def wrap(self, avail_width, avail_height):
        self._width = avail_width
        limit = avail_width - (self.pad_x * 2) - 4
        for line in self.lines:
            width = pdfmetrics.stringWidth(line, "JB", self.size)
            if width > limit:
                raise RuntimeError(
                    f"Code line is {width:.1f}pt wide; frame allows {limit:.1f}pt: {line}"
                )
        self.width = avail_width
        self.height = self.pad_y * 2 + self.leading * len(self.lines)
        if self.height > avail_height and avail_height < PAGE_H:
            # Let the frame move the whole block to the next page.
            return avail_width, self.height
        return avail_width, self.height

    def draw(self):
        canvas = self.canv
        canvas.setFillColor(CODE_BG)
        canvas.setStrokeColor(RULE)
        canvas.setLineWidth(0.6)
        canvas.roundRect(0, 0, self.width, self.height, 3, fill=1, stroke=1)
        canvas.setFillColor(ORANGE)
        canvas.rect(0, 0, 3, self.height, fill=1, stroke=0)
        canvas.setFillColor(BLACK)
        canvas.setFont("JB", self.size)
        y = self.height - self.pad_y - self.size
        for line in self.lines:
            canvas.drawString(self.pad_x + 4, y, line)
            y -= self.leading


def _p(text: str, style: str) -> Paragraph:
    return Paragraph(_esc(text), STYLES[style])


def _rich(html: str, style: str) -> Paragraph:
    return Paragraph(html, STYLES[style])


class SectionHead(Flowable):
    """Heading plus orange rule, so it can sit inside one KeepTogether."""

    def __init__(self, title: str):
        super().__init__()
        self.paragraph = _p(title, "section")

    def wrap(self, avail_width, avail_height):
        _, paragraph_height = self.paragraph.wrap(avail_width, avail_height)
        self.width = avail_width
        self._paragraph_height = paragraph_height
        self.height = 8 + paragraph_height + 6
        return avail_width, self.height

    def draw(self):
        self.paragraph.drawOn(self.canv, 0, 6)
        self.canv.setStrokeColor(ORANGE)
        self.canv.setLineWidth(0.9)
        self.canv.line(0, 2, self.width, 2)


def _section(title: str) -> SectionHead:
    return SectionHead(title)


def _bullets(items: list[str]) -> ListFlowable:
    return ListFlowable(
        [ListItem(_p(item, "bullet"), leftIndent=10, value="square") for item in items],
        bulletType="bullet",
        start="square",
        bulletColor=ORANGE,
        bulletFontName="Inter-Bold",
        bulletFontSize=7,
        leftIndent=12,
        bulletDedent=8,
        spaceBefore=0,
        spaceAfter=2,
    )


def _table(headers: list[str], rows: list[list[str]], col_widths: list[float]) -> Table:
    head = [_rich(f"<font color='white'>{_esc(cell)}</font>", "th") for cell in headers]
    body = []
    for row in rows:
        body.append([_p(cell, "tdm") if i == 0 else _p(cell, "td") for i, cell in enumerate(row)])
    table = Table([head] + body, colWidths=col_widths, repeatRows=1)
    style_cmds = [
        ("BACKGROUND", (0, 0), (-1, 0), BLACK),
        ("TEXTCOLOR", (0, 0), (-1, 0), white),
        ("FONTNAME", (0, 0), (-1, 0), "Inter-Semibold"),
        ("BACKGROUND", (0, 1), (-1, -1), CODE_BG),
        ("ALIGN", (0, 0), (-1, -1), "LEFT"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("GRID", (0, 0), (-1, -1), 0.3, RULE),
        ("LINEBELOW", (0, 0), (-1, 0), 1.5, ORANGE),
    ]
    for index in range(1, len(rows) + 1):
        if index % 2 == 0:
            style_cmds.append(("BACKGROUND", (0, index), (-1, index), ROW_ALT))
    table.setStyle(TableStyle(style_cmds))
    return table


def _header_footer(canvas, doc, lesson_label: str):
    canvas.saveState()
    canvas.setFillColor(OFFWHITE)
    canvas.rect(0, 0, PAGE_W, PAGE_H, fill=1, stroke=0)
    canvas.setFillColor(BLACK)
    canvas.rect(0, PAGE_H - HEADER_H, PAGE_W, HEADER_H, fill=1, stroke=0)
    canvas.setFillColor(ORANGE)
    canvas.rect(0, PAGE_H - HEADER_H - 3, PAGE_W, 3, fill=1, stroke=0)
    canvas.drawImage(
        ImageReader(LOGO),
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
    canvas.drawString(70, PAGE_H - 42, "FREE EDITION  ·  ASSOCIATE FUNDAMENTALS")
    canvas.setFillColor(OFFWHITE)
    canvas.setFont("Inter", 8.5)
    canvas.drawRightString(PAGE_W - 20, PAGE_H - 34, lesson_label)

    canvas.setFillColor(MUTED)
    canvas.setFont("Inter", 8)
    canvas.drawString(LEFT, 16, "buildandteach.com")
    canvas.setFillColor(ORANGE)
    canvas.drawString(LEFT + 92, 16, "·")
    canvas.setFillColor(MUTED)
    canvas.drawString(LEFT + 102, 16, "Quick reference. Not a full script.")
    canvas.drawRightString(PAGE_W - RIGHT, 16, f"{doc.page}")
    canvas.setStrokeColor(RULE)
    canvas.setLineWidth(0.4)
    canvas.line(LEFT, 28, PAGE_W - RIGHT, 28)
    canvas.restoreState()


def _doc(path: Path, story: list, lesson_label: str) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    buffer_pages = {"n": 0}

    def on_page(canvas, doc):
        _header_footer(canvas, doc, lesson_label)
        buffer_pages["n"] = doc.page

    frame = Frame(
        LEFT,
        FOOTER_H + 6,
        CONTENT_W,
        PAGE_H - HEADER_H - 3 - FOOTER_H - 14,
        leftPadding=0,
        rightPadding=0,
        topPadding=0,
        bottomPadding=0,
        showBoundary=0,
    )
    document = BaseDocTemplate(
        str(path),
        pagesize=letter,
        title=lesson_label,
        author="Build and Teach",
        creator="cheatsheets/generate_cheatsheets.py",
    )
    document.addPageTemplates([PageTemplate(id="sheet", frames=[frame], onPage=on_page)])
    document.build(story)
    page_count = buffer_pages["n"]
    if page_count < 1 or page_count > 2:
        raise RuntimeError(f"{path.name} is {page_count} pages; cheat sheets must be 1–2 pages")
    return page_count


def lesson_01() -> list:
    story: list = [
        _p("LESSON 01", "kicker"),
        _p("First Delta Lake table", "title"),
        _p(
            "Managed Delta customers and orders in one demo schema. Read with SQL and PySpark, update one fictional row, then time-travel to the version captured before that update.",
            "lede",
        ),
        _section("Safe writes"),
        _bullets(
            [
                "Schema byte_delta_demo_20261106 in the current catalog only. The setup cell stops if that name or the table names change.",
                "CREATE SCHEMA IF NOT EXISTS and CREATE TABLE IF NOT EXISTS … USING DELTA. No LOCATION and no /mnt path.",
                "A re-run DELETEs rows in the demo customers or orders table, then inserts the sample rows again. It does not DROP or CREATE OR REPLACE.",
                "UPDATE touches only customer_id = 2 with a SAMPLE data note (Alan, Manchester to Cambridge).",
                "pre_update_version is a Python int: max(version) from DESCRIBE HISTORY before the update. Do not hard-code it, and do not call spark.conf.set.",
            ]
        ),
        Spacer(1, 6),
        CodeBlock(
            """
DEMO_SCHEMA = "byte_delta_demo_20261106"
CATALOG = spark.sql(
    "SELECT current_catalog() AS catalog"
).collect()[0]["catalog"]
schema_fqn = f"`{CATALOG}`.`{DEMO_SCHEMA}`"
customers = f"{schema_fqn}.`customers`"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_fqn}")
spark.sql(f\"\"\"
CREATE TABLE IF NOT EXISTS {customers} (
  customer_id INT, name STRING, city STRING,
  signup_date DATE, data_note STRING
) USING DELTA
\"\"\")
"""
        ),
        _section("Sample customers, then one update"),
        _p(
            "Three fictional rows. data_note is SAMPLE — fictional. After the load, Alan is still in Manchester.",
            "body",
        ),
        Spacer(1, 4),
        _table(
            ["id", "name", "city before", "city after update"],
            [
                ["1", "Ada Lovelace", "London", "London"],
                ["2", "Alan Turing", "Manchester", "Cambridge"],
                ["3", "Grace Hopper", "New York", "New York"],
            ],
            [44, 150, 173, 173],
        ),
        Spacer(1, 6),
        CodeBlock(
            """
spark.sql(f"DELETE FROM {customers}")  # demo table only
# then INSERT the three SAMPLE rows, including DATE '1815-12-10'
"""
        ),
        KeepTogether([
            _section("Reads"),
            CodeBlock(
                """
display(spark.sql(f\"\"\"
SELECT customer_id, name, city, signup_date, data_note
FROM {customers}
ORDER BY customer_id
\"\"\"))
display(spark.table(customers).orderBy("customer_id"))
display(spark.table(customers).filter("city = 'London'"))  # Ada
"""
            ),
        ]),
        KeepTogether([
            _section("Capture the version, then update"),
            _p(
                "DELETE and INSERT add commits, so the version number changes every load. Read max(version) first, store it in pre_update_version, then update.",
                "body",
            ),
            Spacer(1, 4),
            CodeBlock(
                """
hist = spark.sql(f"DESCRIBE HISTORY {customers}")
pre_update_version = int(
    hist.selectExpr("max(version) AS v").collect()[0]["v"]
)
spark.sql(f\"\"\"
UPDATE {customers}
SET city = 'Cambridge'
WHERE customer_id = 2
  AND data_note LIKE 'SAMPLE%'
\"\"\")
"""
            ),
        ]),
        KeepTogether([
            _section("Time travel back to that version"),
            CodeBlock(
                """
spark.sql(f\"\"\"
SELECT customer_id, name, city, data_note
FROM {customers} VERSION AS OF {pre_update_version}
ORDER BY customer_id
\"\"\")
df_prior = (
    spark.read.format("delta")
    .option("versionAsOf", str(pre_update_version))
    .table(customers)
)
"""
            ),
        ]),
        _section("What to check"),
        _bullets(
            [
                "SQL and PySpark both show three rows. The London filter shows Ada only.",
                "After UPDATE, Alan's city is Cambridge. Ada and Grace stay put. VERSION AS OF the captured number still shows Manchester.",
                "DESCRIBE HISTORY includes an UPDATE. The printed version is whatever max(version) was on this run.",
                "Orders practice uses the same schema. Amounts are CAST(n AS DOUBLE) because serverless ANSI SQL treats a bare 42.50 as DECIMAL. Ada appears twice. Grace has no order.",
            ]
        ),
        Spacer(1, 4),
        CodeBlock(
            """
spark.sql(f\"\"\"
INSERT INTO {orders} VALUES
  (1001, 1, CAST(42.50 AS DOUBLE), 'SAMPLE — fictional'),
  (1002, 2, CAST(19.99 AS DOUBLE), 'SAMPLE — fictional'),
  (1003, 1, CAST(7.25 AS DOUBLE), 'SAMPLE — fictional')
\"\"\")
"""
        ),
    ]
    return story


def lesson_02() -> list:
    story: list = [
        _p("LESSON 02", "kicker"),
        _p("Clean and transform with PySpark", "title"),
        _p(
            "Eight fictional orders. Choose columns, reject invalid rows, label missing text, derive a decimal total, and save a new managed Delta table with mode(\"error\").",
            "lede",
        ),
        _section("Cleaning rules"),
        _bullets(
            [
                "select builds a new DataFrame without internal_note. raw_orders still has that column and still has 8 rows.",
                "Do not fill a missing quantity or price with zero. That would invent a sale.",
                "Keep a row only when order_id, quantity, and unit_price are present, quantity > 0, and unit_price >= 0. A zero price is allowed. A negative quantity is not a return in this lesson.",
                "Labels use trim. Null or blank becomes Unknown. Region is then uppercased. The helper returns a Spark column expression, not a Python UDF.",
                "order_total is quantity * unit_price, cast to decimal(12,2). orderBy affects the display, not the physical order of the saved table.",
            ]
        ),
        Spacer(1, 5),
        CodeBlock(
            """
valid = (
    F.col("order_id").isNotNull()
    & F.col("quantity").isNotNull()
    & F.col("unit_price").isNotNull()
    & (F.col("quantity") > 0)
    & (F.col("unit_price") >= 0)
)
accepted = selected.filter(valid)
rejected = selected.filter(~valid)
"""
        ),
        Spacer(1, 6),
        CodeBlock(
            """
def label_or_unknown(column_name):
    value = F.trim(F.col(column_name))
    return F.when(
        value.isNull() | (value == ""), F.lit("Unknown")
    ).otherwise(value)

clean_orders = (
    accepted
    .withColumn("customer", label_or_unknown("customer"))
    .withColumn("region", F.upper(label_or_unknown("region")))
    .withColumn(
        "order_total",
        (F.col("quantity") * F.col("unit_price")).cast("decimal(12,2)"),
    )
)
"""
        ),
        Spacer(1, 8),
        _p("What is wrong in the eight raw rows", "body"),
        Spacer(1, 3),
        _table(
            ["id", "raw issue"],
            [
                ["1001", "customer is ' Ana ' with surrounding spaces"],
                ["1002", "quantity is null — reject, do not invent 0"],
                ["1003", "customer is null — later labeled Unknown"],
                ["1004", "quantity is -1 — reject"],
                ["1005", "unit_price is null — reject, do not invent a free sale"],
                ["1006", "valid; internal_note is null and is not selected"],
                ["1007", "valid west order"],
                ["1008", "region is blank — later labeled UNKNOWN"],
            ],
            [52, 488],
        ),
        CondPageBreak(300),
        _section("Safe Delta write"),
        _bullets(
            [
                "Schema name is bt_clean_orders_ plus 12 hex characters from uuid4, in the current catalog. A full rerun creates another schema. It does not replace the previous one.",
                "CREATE SCHEMA, then one managed table clean_orders. No LOCATION, no /mnt, no DROP, no spark.conf.",
                "Write with format(\"delta\").mode(\"error\"). That fails if the table already exists. Do not change it to overwrite.",
                "Keep the spelling error. On Free Edition Spark Connect, the alias errorifexists was rejected during serialization, before a table was created.",
                "If the write cell has already succeeded, skip it and run the SQL check. The notebook does not delete the demo schema for you.",
            ]
        ),
        Spacer(1, 5),
        CodeBlock(
            """
catalog = spark.sql("SELECT current_catalog()").first()[0]
demo_schema = "bt_clean_orders_" + uuid4().hex[:12]
table_fqn = f"`{catalog}`.`{demo_schema}`.`clean_orders`"
# CREATE SCHEMA {quoted schema} COMMENT '...fictional orders demo'
clean_orders.write.format("delta").mode("error").saveAsTable(table_fqn)
"""
        ),
        _section("Verify the stored table"),
        _p(
            "Read the fully qualified table. In-memory checks and the SQL summary both expect 5 rows and total_sales 66.00. Persisted order_id values are 1001, 1003, 1006, 1007, 1008.",
            "body",
        ),
        Spacer(1, 4),
        CodeBlock(
            """
summary = spark.sql(f\"\"\"
SELECT COUNT(*) AS row_count,
       SUM(order_total) AS total_sales
FROM {table_fqn}
\"\"\")
# row_count == 5
# total_sales == Decimal("66.00")
"""
        ),
        Spacer(1, 6),
        _table(
            ["id", "customer", "region", "qty", "price", "total"],
            [
                ["1001", "Ana", "NORTH", "2", "10.00", "20.00"],
                ["1003", "Unknown", "EAST", "3", "7.50", "22.50"],
                ["1006", "Fay", "NORTH", "1", "12.00", "12.00"],
                ["1007", "Gus", "WEST", "2", "4.25", "8.50"],
                ["1008", "Hana", "UNKNOWN", "1", "3.00", "3.00"],
            ],
            [64, 100, 110, 56, 90, 120],
        ),
        Spacer(1, 8),
        _p("Rejected rows stay out of the table. Inspect them; do not zero-fill them.", "body"),
        Spacer(1, 4),
        _table(
            ["id", "customer", "why it is rejected"],
            [
                ["1002", "Bo", "quantity is null"],
                ["1004", "Dee", "quantity is -1"],
                ["1005", "Eli", "unit_price is null"],
            ],
            [64, 100, 376],
        ),
        Spacer(1, 6),
        _p(
            "Free Edition run: 8 raw, 5 clean, 3 rejected, total 66.00. All 8 code cells passed. Ana is trimmed from ' Ana '. The blank region on 1008 becomes UNKNOWN.",
            "body",
        ),
        Spacer(1, 3),
        _p(
            "Practice, without writing: NORTH rows with order_total >= 15, columns order_id, customer, order_total. Expected row: 1001, Ana, 20.00.",
            "body",
        ),
    ]
    return story


LESSONS = [
    (
        "Lesson 01",
        ROOT / "lessons/associate-fundamentals/01-first-delta-lake-table/cheatsheet.pdf",
        lesson_01,
        [
            "byte_delta_demo_20261106",
            "pre_update_version",
            "VERSION AS OF",
            "versionAsOf",
            "Cambridge",
            "Manchester",
            "SAMPLE",
            "CREATE TABLE IF NOT EXISTS",
            "CAST",
            "spark.conf.set",
        ],
    ),
    (
        "Lesson 02",
        ROOT / "lessons/associate-fundamentals/02-clean-transform-pyspark/cheatsheet.pdf",
        lesson_02,
        [
            'mode("error")',
            "bt_clean_orders_",
            "66.00",
            "1001",
            "1003",
            "1006",
            "1007",
            "1008",
            "Unknown",
            "decimal(12,2)",
            "errorifexists",
            "8 raw, 5 clean, 3 rejected",
        ],
    ),
]


def _check(path: Path, required: list[str]) -> None:
    """Fail if text is missing or ink runs into the page edge."""
    from pypdf import PdfReader
    import pypdfium2 as pdfium

    reader = PdfReader(str(path))
    if not 1 <= len(reader.pages) <= 2:
        raise RuntimeError(f"{path.name} has {len(reader.pages)} pages")
    text = "\n".join((page.extract_text() or "") for page in reader.pages)
    missing = [phrase for phrase in required if phrase not in text]
    if missing:
        raise RuntimeError(f"{path.name} is missing extracted text: {missing}")
    document = pdfium.PdfDocument(str(path))
    offwhite = (246, 243, 238)
    for index, page in enumerate(document, start=1):
        scale = 2
        image = page.render(scale=scale).to_pil().convert("RGB")
        width, height = image.size
        pixels = image.load()
        header_px = int(61 * scale)
        footer_px = int(32 * scale)
        edge_hits = 0
        for y in range(header_px + 2, height - footer_px - 2):
            for x in (0, 1, width - 2, width - 1):
                red, green, blue = pixels[x, y]
                if any(abs(channel - target) > 24 for channel, target in zip((red, green, blue), offwhite)):
                    edge_hits += 1
        if edge_hits:
            raise RuntimeError(f"{path.name} page {index} has {edge_hits} non-background edge pixels")
        last_ink = header_px
        margin = int(24 * scale)
        for y in range(header_px, height - footer_px):
            if any(
                any(abs(channel - target) > 14 for channel, target in zip(pixels[x, y], offwhite))
                for x in range(margin, width - margin, 5)
            ):
                last_ink = y
        gap_pt = ((height - footer_px) - last_ink) / scale
        if gap_pt < 8:
            raise RuntimeError(f"{path.name} page {index} content is {gap_pt:.1f}pt from the footer")


def main() -> None:
    if not LOGO_PATH.is_file():
        raise SystemExit(f"Missing logo: {LOGO_PATH}")
    for label, path, builder, required in LESSONS:
        pages = _doc(path, builder(), label)
        _check(path, required)
        print(f"wrote {path.relative_to(ROOT)} ({pages} page{'s' if pages != 1 else ''})")
    print(f"sheets: {len(LESSONS)}")


if __name__ == "__main__":
    main()
