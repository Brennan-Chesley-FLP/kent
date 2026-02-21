"""Bug Civil Court demo website.

A FastAPI application that serves the Bug Civil Court — a whimsical
fictional court where insects file civil lawsuits.  Most content is
rendered as server-side HTML (requiring the scraper to parse it); justice
bios are also available as a JSON API.
"""

from __future__ import annotations

from datetime import date

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse

from kent.demo.data import (
    CASES,
    CASES_BY_DOCKET,
    JUSTICES,
    JUSTICES_BY_SLUG,
    DemoCase,
    Justice,
    get_case,
)

app = FastAPI(title="Bug Civil Court", version="1.0.0")

# ── HTML helpers ────────────────────────────────────────────────────

_CSS = """\
body { font-family: Georgia, serif; max-width: 960px; margin: 2em auto;
       padding: 0 1em; color: #333; background: #fffff8; }
h1 { color: #2a5934; border-bottom: 2px solid #2a5934; padding-bottom: .3em; }
h2 { color: #4a7856; }
nav { background: #2a5934; padding: .8em 1.2em; margin-bottom: 1.5em;
      border-radius: 4px; }
nav a { color: #fff; text-decoration: none; margin-right: 1.5em;
        font-weight: bold; }
nav a:hover { text-decoration: underline; }
table { border-collapse: collapse; width: 100%; margin: 1em 0; }
th, td { border: 1px solid #ccc; padding: .5em .8em; text-align: left; }
th { background: #e8f0e8; }
tr:nth-child(even) { background: #f9f9f5; }
.case-details dl { display: grid; grid-template-columns: 200px 1fr;
                    gap: .3em 1em; }
.case-details dt { font-weight: bold; color: #555; }
.case-details dd { margin: 0; }
.documents { margin-top: 1.5em; }
.documents a { display: inline-block; margin-right: 1.5em;
               padding: .4em .8em; background: #2a5934; color: #fff;
               border-radius: 3px; text-decoration: none; }
.documents a:hover { background: #1d3d24; }
.justice-card { border: 1px solid #ccc; border-radius: 6px;
                padding: 1em; margin: 1em 0; display: flex; gap: 1.5em; }
.justice-card img { width: 120px; height: 120px; object-fit: cover;
                     border-radius: 50%; }
.justice-info h3 { margin: 0 0 .3em; }
.justice-info .species { color: #777; font-style: italic; }
a.case-link { color: #2a5934; }
footer { margin-top: 3em; border-top: 1px solid #ccc; padding-top: 1em;
         color: #999; font-size: .85em; text-align: center; }
"""


def _page(title: str, body: str) -> HTMLResponse:
    html = f"""\
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{title} — Bug Civil Court</title>
  <style>{_CSS}</style>
</head>
<body>
  <nav>
    <a href="/">Home</a>
    <a href="/cases">Cases</a>
    <a href="/cases/search">Search</a>
    <a href="/opinions">Opinions</a>
    <a href="/oral-arguments">Oral Arguments</a>
    <a href="/justices">Justices</a>
  </nav>
  {body}
  <footer>
    Bug Civil Court &mdash; a kent framework demo
    &bull; All proceedings are fictional and no insects were harmed.
  </footer>
</body>
</html>"""
    return HTMLResponse(content=html)


# ── Routes: Home ────────────────────────────────────────────────────


@app.get("/", response_class=HTMLResponse)
async def homepage():
    body = """\
<h1>Bug Civil Court</h1>
<p><em>"Justice is blind, but she has compound eyes."</em></p>

<p>Welcome to the Bug Civil Court, the premier judicial institution
for the resolution of disputes among Arthropoda. Since its founding,
this court has adjudicated matters ranging from property tunneling to
identity theft via metamorphosis.</p>

<h2>Quick Links</h2>
<ul>
  <li><a href="/cases">Browse all cases</a> (2024 &ndash; 2026)</li>
  <li><a href="/opinions">Read published opinions</a></li>
  <li><a href="/oral-arguments">Listen to oral arguments</a></li>
  <li><a href="/justices">Meet the justices</a></li>
  <li><a href="/api/justices">Justice bios (JSON API)</a></li>
</ul>
"""
    return _page("Home", body)


# ── Routes: Cases ───────────────────────────────────────────────────


def _case_row(c: DemoCase) -> str:
    return f"""\
<tr class="case-row" data-docket="{c.docket}">
  <td class="docket"><a class="case-link" \
href="/cases/{c.date_filed.year}/{int(c.docket.split("-")[-1])}">\
{c.docket}</a></td>
  <td class="case-name">{c.case_name}</td>
  <td class="date-filed">{c.date_filed.isoformat()}</td>
  <td class="case-type">{c.case_type}</td>
  <td class="status">{c.status}</td>
</tr>"""


@app.get("/cases", response_class=HTMLResponse)
async def cases_list():
    rows = "\n".join(_case_row(c) for c in CASES)
    body = f"""\
<h1>Case Search Results</h1>
<p>Showing all {len(CASES)} cases filed in Bug Civil Court.</p>
<table id="cases-table">
  <thead>
    <tr>
      <th>Docket</th><th>Case Name</th><th>Date Filed</th>
      <th>Type</th><th>Status</th>
    </tr>
  </thead>
  <tbody>
    {rows}
  </tbody>
</table>"""
    return _page("Cases", body)


_SEARCH_FORM = """\
<form id="date-search" method="GET" action="/cases/search">
  <label for="from_date">From:</label>
  <input type="date" id="from_date" name="from_date" value="{from_date}">
  <label for="to_date">To:</label>
  <input type="date" id="to_date" name="to_date" value="{to_date}">
  <button type="submit">Search</button>
</form>"""


@app.get("/cases/search", response_class=HTMLResponse)
async def cases_search(
    from_date: str | None = Query(None),
    to_date: str | None = Query(None),
):
    form_html = _SEARCH_FORM.format(
        from_date=from_date or "",
        to_date=to_date or "",
    )

    if not from_date and not to_date:
        body = f"""\
<h1>Search Cases by Date Filed</h1>
{form_html}
<p>Enter a date range and click Search.</p>"""
        return _page("Search Cases", body)

    start = date.fromisoformat(from_date) if from_date else date.min
    end = date.fromisoformat(to_date) if to_date else date.max
    matched = [c for c in CASES if start <= c.date_filed <= end]

    rows = "\n".join(_case_row(c) for c in matched)
    body = f"""\
<h1>Search Cases by Date Filed</h1>
{form_html}
<p>Found {len(matched)} case{"" if len(matched) == 1 else "s"}.</p>
<table id="cases-table">
  <thead>
    <tr>
      <th>Docket</th><th>Case Name</th><th>Date Filed</th>
      <th>Type</th><th>Status</th>
    </tr>
  </thead>
  <tbody>
    {rows}
  </tbody>
</table>"""
    return _page("Search Results", body)


@app.get("/cases/{year}", response_class=HTMLResponse)
async def cases_by_year(year: int):
    year_cases = [c for c in CASES if c.date_filed.year == year]
    if not year_cases:
        raise HTTPException(404, f"No cases found for year {year}")
    rows = "\n".join(_case_row(c) for c in year_cases)
    body = f"""\
<h1>Cases Filed in {year}</h1>
<p>Showing {len(year_cases)} cases.</p>
<table id="cases-table">
  <thead>
    <tr>
      <th>Docket</th><th>Case Name</th><th>Date Filed</th>
      <th>Type</th><th>Status</th>
    </tr>
  </thead>
  <tbody>
    {rows}
  </tbody>
</table>"""
    return _page(f"Cases — {year}", body)


@app.get("/cases/{year}/{number}", response_class=HTMLResponse)
async def case_detail(year: int, number: int):
    case = get_case(year, number)
    if case is None:
        return _page(
            "Case Not Found",
            "<h1>Case Not Found</h1>"
            f"<p>No case found for docket BCC-{year}-{number:03d}.</p>",
        )

    opinion_link = ""
    if case.has_opinion:
        opinion_link = (
            f'<a class="opinion-link" '
            f'href="/opinions/{case.docket}">Read Opinion</a>'
        )

    oral_arg_link = ""
    if case.has_oral_argument:
        oral_arg_link = (
            f'<a class="audio-link" '
            f'href="/oral-arguments/{case.docket}">'
            f"Listen to Oral Argument</a>"
        )

    body = f"""\
<h1>Bug Civil Court</h1>
<h2>{case.case_name}</h2>

<div class="case-details">
  <dl>
    <dt>Docket Number</dt>
    <dd id="docket">{case.docket}</dd>

    <dt>Plaintiff</dt>
    <dd id="plaintiff">{case.plaintiff}</dd>

    <dt>Defendant</dt>
    <dd id="defendant">{case.defendant}</dd>

    <dt>Date Filed</dt>
    <dd id="date-filed">{case.date_filed.isoformat()}</dd>

    <dt>Case Type</dt>
    <dd id="case-type">{case.case_type}</dd>

    <dt>Status</dt>
    <dd id="status">{case.status}</dd>

    <dt>Presiding Judge</dt>
    <dd id="judge">{case.judge}</dd>

    <dt>Case Summary</dt>
    <dd id="summary">{case.summary}</dd>
  </dl>
</div>

<div class="documents">
  <h3>Documents</h3>
  {opinion_link}
  {oral_arg_link}
  {
        (
            "<p>No documents on file.</p>"
            if not opinion_link and not oral_arg_link
            else ""
        )
    }
</div>"""
    return _page(case.case_name, body)


# ── Routes: Opinions ────────────────────────────────────────────────


@app.get("/opinions", response_class=HTMLResponse)
async def opinions_list():
    opinion_cases = [c for c in CASES if c.has_opinion]
    rows = []
    for c in opinion_cases:
        rows.append(
            f'<tr class="opinion-row">'
            f'<td><a href="/opinions/{c.docket}">{c.docket}</a></td>'
            f"<td>{c.case_name}</td>"
            f"<td>{c.status}</td>"
            f"<td>{c.judge}</td>"
            f"</tr>"
        )
    body = f"""\
<h1>Published Opinions</h1>
<p>{len(opinion_cases)} opinions on file.</p>
<table id="opinions-table">
  <thead>
    <tr><th>Docket</th><th>Case</th><th>Status</th><th>Author</th></tr>
  </thead>
  <tbody>
    {"".join(rows)}
  </tbody>
</table>"""
    return _page("Opinions", body)


@app.get("/opinions/{docket}", response_class=HTMLResponse)
async def opinion_detail(docket: str):
    case = CASES_BY_DOCKET.get(docket)
    if case is None or not case.has_opinion:
        raise HTTPException(404, "Opinion not found")

    body = f"""\
<h1>Opinion — {case.case_name}</h1>
<div class="opinion-details">
  <dl>
    <dt>Docket</dt>
    <dd id="docket">{case.docket}</dd>
    <dt>Case</dt>
    <dd id="case-name">{case.case_name}</dd>
    <dt>Author</dt>
    <dd id="judge">{case.judge}</dd>
  </dl>
</div>
<div class="documents">
  <h3>Opinion Document</h3>
  <a class="opinion-image-link" href="{case.opinion_image_url}">\
Download Illustration</a>
  <p><img src="{case.opinion_image_url}" alt="Opinion illustration" \
style="max-width:320px; margin-top:1em;"></p>
</div>"""
    return _page(f"Opinion — {case.docket}", body)


# ── Routes: Oral Arguments ──────────────────────────────────────────


@app.get("/oral-arguments", response_class=HTMLResponse)
async def oral_arguments_list():
    oa_cases = [c for c in CASES if c.has_oral_argument]
    rows = []
    for c in oa_cases:
        rows.append(
            f'<tr class="oral-arg-row">'
            f'<td><a href="/oral-arguments/{c.docket}">{c.docket}</a></td>'
            f"<td>{c.case_name}</td>"
            f"<td>{c.date_filed.isoformat()}</td>"
            f"</tr>"
        )
    body = f"""\
<h1>Oral Arguments</h1>
<p>{len(oa_cases)} recordings on file.</p>
<table id="oral-arguments-table">
  <thead>
    <tr><th>Docket</th><th>Case</th><th>Date</th></tr>
  </thead>
  <tbody>
    {"".join(rows)}
  </tbody>
</table>"""
    return _page("Oral Arguments", body)


@app.get("/oral-arguments/{docket}", response_class=HTMLResponse)
async def oral_argument_detail(docket: str):
    case = CASES_BY_DOCKET.get(docket)
    if case is None or not case.has_oral_argument:
        raise HTTPException(404, "Oral argument not found")

    body = f"""\
<h1>Oral Argument — {case.case_name}</h1>
<div class="oral-arg-details">
  <dl>
    <dt>Docket</dt>
    <dd id="docket">{case.docket}</dd>
    <dt>Case</dt>
    <dd id="case-name">{case.case_name}</dd>
    <dt>Date</dt>
    <dd id="date-filed">{case.date_filed.isoformat()}</dd>
  </dl>
</div>
<div class="documents">
  <h3>Audio Recording</h3>
  <a class="audio-download-link" \
href="{case.oral_argument_audio_url}">Download Audio (WAV)</a>
  <p><audio controls src="{case.oral_argument_audio_url}"></audio></p>
</div>"""
    return _page(f"Oral Argument — {case.docket}", body)


# ── Routes: Justices (HTML) ─────────────────────────────────────────


def _justice_card(j: Justice) -> str:
    return f"""\
<div class="justice-card" data-slug="{j.slug}">
  <img src="{j.image_url}" alt="Portrait of {j.name}">
  <div class="justice-info">
    <h3>{j.name}</h3>
    <p class="species">{j.insect_species}</p>
    <p><strong>{j.title}</strong> &mdash;
       Appointed {j.appointed_date.isoformat()}</p>
    <p>{j.bio}</p>
    <p><em>Notable opinion:</em> {j.notable_opinion}</p>
  </div>
</div>"""


@app.get("/justices", response_class=HTMLResponse)
async def justices_page():
    cards = "\n".join(_justice_card(j) for j in JUSTICES)
    body = f"""\
<h1>Justices of the Bug Civil Court</h1>
<p><em>"We are seven—and we have 42 legs between us, not counting
the honorary arachnids."</em></p>
{cards}"""
    return _page("Justices", body)


# ── Routes: Justices (JSON API) ─────────────────────────────────────


def _justice_dict(j: Justice) -> dict:
    return {
        "name": j.name,
        "slug": j.slug,
        "insect_species": j.insect_species,
        "title": j.title,
        "appointed_date": j.appointed_date.isoformat(),
        "bio": j.bio,
        "image_url": j.image_url,
        "notable_opinion": j.notable_opinion,
    }


@app.get("/api/justices", response_class=JSONResponse)
async def api_justices():
    return [_justice_dict(j) for j in JUSTICES]


@app.get("/api/justices/{slug}", response_class=JSONResponse)
async def api_justice_detail(slug: str):
    j = JUSTICES_BY_SLUG.get(slug)
    if j is None:
        raise HTTPException(404, "Justice not found")
    return _justice_dict(j)
