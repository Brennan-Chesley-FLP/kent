"""Fixture data for the Bug Civil Court demo.

This module is the single source of truth for all demo content.
Both the demo website and the expected-output fixture are derived
from the dataclasses defined here.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

# ── External media URLs ─────────────────────────────────────────────

USDA_BASE = "https://www.ars.usda.gov/ARSUserFiles/3559"
PSU_IMG = (
    "https://ento.psu.edu/outreach/extension/insect-image-gallery/all-images"
)

AUDIO_URLS: list[str] = [
    f"{USDA_BASE}/termite1.wav",
    f"{USDA_BASE}/mosquito.wav",
    f"{USDA_BASE}/fireants_shortclip.wav",
    f"{USDA_BASE}/molcrik1.wav",
    f"{USDA_BASE}/grubsnd.wav",
    f"{USDA_BASE}/sawfly.wav",
    f"{USDA_BASE}/formosa1.wav",
    f"{USDA_BASE}/albsounds.wav",
    f"{USDA_BASE}/cffcall2.wav",
    f"{USDA_BASE}/medfly11.wav",
    f"{USDA_BASE}/polyphylla.wav",
    f"{USDA_BASE}/molecricket4-sept02.wav",
]

IMAGE_URLS: list[str] = [
    # Japanese beetle
    f"{PSU_IMG}/japanese-beetle-adult.jpg",
    # Eastern tiger swallowtail butterfly
    f"{PSU_IMG}/eastern-tiger-swallowtail.jpg",
    # Monarch butterfly larva
    f"{PSU_IMG}/monarch-butterfly-larva.jpg",
    # Multicolored Asian lady beetle
    f"{PSU_IMG}/multicolored-asian-lady-beetle-adult.jpg",
    # Io moth (wings open)
    f"{PSU_IMG}/io-moth-wings-open.jpg",
    # Bumble bee on raspberry flower
    f"{PSU_IMG}/bumble-bee-on-raspberry-flower.jpg",
    # Praying mantid
    f"{PSU_IMG}/praying-mantid.jpg",
    # Hermit flower beetle
    f"{PSU_IMG}/hermit-flower-beetle.jpg",
    # Imperial moth
    f"{PSU_IMG}/imperial-moth.jpg",
    # Eastern subterranean termite
    f"{PSU_IMG}/eastern-subterranean-termite.jpg",
    # Carpenter bee (male)
    f"{PSU_IMG}/carpenter-bee-male.jpg",
    # Polyphemus moth
    f"{PSU_IMG}/polyphemus-moth-adult.jpg",
]


# ── Justice data ────────────────────────────────────────────────────


@dataclass(frozen=True)
class Justice:
    name: str
    slug: str
    insect_species: str
    title: str
    appointed_date: date
    bio: str
    image_url: str
    notable_opinion: str


JUSTICES: list[Justice] = [
    Justice(
        name="Hon. Mantis Green",
        slug="mantis-green",
        insect_species="Praying Mantis (Mantis religiosa)",
        title="Chief Justice",
        appointed_date=date(2018, 3, 15),
        bio=(
            "A measured jurist known for patience—she will sit motionless "
            "for hours before striking down a bad argument. Author of the "
            "landmark opinion in *Leaf v. Twig* establishing that "
            "camouflage does not constitute fraud."
        ),
        image_url=IMAGE_URLS[6],  # mantis
        notable_opinion="Leaf v. Twig (2020)",
    ),
    Justice(
        name="Hon. Dragonfly Swift",
        slug="dragonfly-swift",
        insect_species="Emperor Dragonfly (Anax imperator)",
        title="Associate Justice",
        appointed_date=date(2019, 6, 1),
        bio=(
            "Former aerial-patrol officer turned jurist, Justice Swift "
            "is renowned for swift deliberation and compound-eyed "
            "attention to detail. His opinions in airspace-trespass law "
            "are cited across all six legs of the judiciary."
        ),
        image_url=IMAGE_URLS[4],  # dragonfly
        notable_opinion="Pond Sovereignty Act Advisory (2021)",
    ),
    Justice(
        name="Hon. Cricket Chirp",
        slug="cricket-chirp",
        insect_species="Field Cricket (Gryllus campestris)",
        title="Associate Justice",
        appointed_date=date(2019, 9, 22),
        bio=(
            "A nocturnal scholar whose tireless work ethic—and soothing "
            "stridulation—keep the court running after dark. Known for "
            "his dissent in *Cicada v. Cricket*, in which he argued that "
            "'one bug's noise is another bug's music.'"
        ),
        image_url=IMAGE_URLS[9],  # cicada (close enough)
        notable_opinion="Cicada v. Cricket dissent (2024)",
    ),
    Justice(
        name="Hon. Silkworm Weaver",
        slug="silkworm-weaver",
        insect_species="Silkworm Moth (Bombyx mori)",
        title="Associate Justice",
        appointed_date=date(2020, 1, 10),
        bio=(
            "Justice Weaver spins intricate legal reasoning with the "
            "care of one who has literally spun silk. Specialist in "
            "contract law, her opinions are tightly woven and almost "
            "impossible to unravel on appeal."
        ),
        image_url=IMAGE_URLS[11],  # atlas moth
        notable_opinion="Thread of Commerce doctrine (2022)",
    ),
    Justice(
        name="Hon. Bombardier Burns",
        slug="bombardier-burns",
        insect_species="Bombardier Beetle (Brachinus crepitans)",
        title="Associate Justice",
        appointed_date=date(2021, 4, 18),
        bio=(
            "A fiery temperament matched by an equally explosive legal "
            "style. Justice Burns is the court's foremost authority on "
            "self-defense doctrine, drawing from personal experience "
            "with exothermic chemical reactions."
        ),
        image_url=IMAGE_URLS[7],  # stag beetle
        notable_opinion="Castle Doctrine for Burrows (2023)",
    ),
    Justice(
        name="Hon. Luna Moth",
        slug="luna-moth",
        insect_species="Luna Moth (Actias luna)",
        title="Associate Justice",
        appointed_date=date(2022, 7, 4),
        bio=(
            "The court's most ethereal member, Justice Moth brings a "
            "luminous perspective to night-court proceedings. Critics "
            "say she is drawn to bright arguments; admirers note that "
            "her brief adult tenure only sharpens her sense of urgency."
        ),
        image_url=IMAGE_URLS[8],  # morpho butterfly
        notable_opinion="Right to Light (2024)",
    ),
    Justice(
        name="Hon. Katydid Kafka",
        slug="katydid-kafka",
        insect_species="True Katydid (Pterophylla camellifolia)",
        title="Associate Justice",
        appointed_date=date(2023, 1, 3),
        bio=(
            "The newest member of the bench, Justice Kafka specializes "
            "in cases involving metamorphosis and transformation. Widely "
            "considered the court's most literary voice, she once opened "
            "an opinion with 'One morning, the defendant awoke to find "
            "himself transformed into a liable party.'"
        ),
        image_url=IMAGE_URLS[2],  # monarch butterfly
        notable_opinion="Metamorphosis Liability Standard (2024)",
    ),
]


# ── Case data ───────────────────────────────────────────────────────


@dataclass(frozen=True)
class DemoCase:
    docket: str
    case_name: str
    plaintiff: str
    defendant: str
    date_filed: date
    case_type: str
    status: str
    judge: str
    summary: str
    has_opinion: bool = False
    has_oral_argument: bool = False
    opinion_image_url: str = ""
    oral_argument_audio_url: str = ""


def _docket(year: int, n: int) -> str:
    return f"BCC-{year}-{n:03d}"


# fmt: off
CASES: list[DemoCase] = [
    # ── 2024 ─────────────────────────────────────────────────────
    DemoCase(
        docket=_docket(2024, 1),
        case_name="Beetle v. Ant Colony",
        plaintiff="Barry Beetle",
        defendant="Ant Colony No. 47",
        date_filed=date(2024, 1, 15),
        case_type="Property Dispute",
        status="Closed",
        judge="Hon. Mantis Green",
        summary=(
            "Plaintiff alleges defendant tunneled under his log without "
            "permission, causing subsidence and structural cracks in his "
            "prized bark collection."
        ),
        has_opinion=True,
        opinion_image_url=IMAGE_URLS[0],
        has_oral_argument=True,
        oral_argument_audio_url=AUDIO_URLS[0],
    ),
    DemoCase(
        docket=_docket(2024, 2),
        case_name="Butterfly v. Caterpillar",
        plaintiff="Monarch Butterfly",
        defendant="Carl Caterpillar",
        date_filed=date(2024, 2, 1),
        case_type="Identity Theft",
        status="Closed",
        judge="Hon. Katydid Kafka",
        summary=(
            "Plaintiff claims defendant illegally assumed their identity "
            "during metamorphosis, a novel question of post-pupal "
            "personhood."
        ),
        has_opinion=True,
        opinion_image_url=IMAGE_URLS[2],
        has_oral_argument=True,
        oral_argument_audio_url=AUDIO_URLS[1],
    ),
    DemoCase(
        docket=_docket(2024, 3),
        case_name='Spider v. Fly ("Will You Walk Into My Parlour?")',
        plaintiff="Webster Spider",
        defendant="Freddy Fly",
        date_filed=date(2024, 2, 14),
        case_type="Breach of Contract",
        status="Pending",
        judge="Hon. Silkworm Weaver",
        summary=(
            "Plaintiff alleges defendant breached the web-visiting "
            "agreement by arriving and immediately departing. The "
            "contract clearly states 'said the spider to the fly.'"
        ),
    ),
    DemoCase(
        docket=_docket(2024, 4),
        case_name="Grasshopper v. Ant",
        plaintiff="Gary Grasshopper",
        defendant="Andy Ant",
        date_filed=date(2024, 3, 1),
        case_type="Defamation",
        status="Closed",
        judge="Hon. Cricket Chirp",
        summary=(
            "Plaintiff claims defendant spread the Aesop fable to "
            "damage his reputation, calling him 'lazy' when in fact "
            "he is a freelance musician."
        ),
        has_opinion=True,
        opinion_image_url=IMAGE_URLS[3],
    ),
    DemoCase(
        docket=_docket(2024, 5),
        case_name="Bee v. Wasp",
        plaintiff="Beatrice Bee",
        defendant="Walter Wasp",
        date_filed=date(2024, 3, 15),
        case_type="Assault",
        status="Pending",
        judge="Hon. Dragonfly Swift",
        summary=(
            "Plaintiff alleges unprovoked stinging incident at the "
            "Community Flower Garden. Defendant claims it was a case "
            "of mistaken identity—all yellow-and-black insects look "
            "alike to him."
        ),
        has_oral_argument=True,
        oral_argument_audio_url=AUDIO_URLS[2],
    ),
    DemoCase(
        docket=_docket(2024, 6),
        case_name="Ladybug v. Aphid",
        plaintiff="Lucy Ladybug",
        defendant="Arthur Aphid",
        date_filed=date(2024, 4, 1),
        case_type="Nuisance",
        status="Closed",
        judge="Hon. Mantis Green",
        summary=(
            "Plaintiff seeks a restraining order citing persistent "
            "plant damage in her rose garden. Defendant counters that "
            "he has a constitutional right to feed."
        ),
        has_opinion=True,
        opinion_image_url=IMAGE_URLS[3],
        has_oral_argument=True,
        oral_argument_audio_url=AUDIO_URLS[3],
    ),
    DemoCase(
        docket=_docket(2024, 7),
        case_name="Firefly v. Moth",
        plaintiff="Flash Firefly",
        defendant="Dusty Moth",
        date_filed=date(2024, 4, 15),
        case_type="Intellectual Property",
        status="Pending",
        judge="Hon. Luna Moth",
        summary=(
            "Plaintiff claims defendant copied his patented "
            "bioluminescent signaling patterns, leading to confusion "
            "among potential mates at the meadow mixer."
        ),
    ),
    DemoCase(
        docket=_docket(2024, 8),
        case_name="Dragonfly v. Mosquito",
        plaintiff="Dana Dragonfly",
        defendant="Mike Mosquito",
        date_filed=date(2024, 5, 1),
        case_type="Trespass",
        status="Pending",
        judge="Hon. Dragonfly Swift",
        summary=(
            "Plaintiff alleges repeated unauthorized entry into her "
            "pond territory. Judge Swift has recused himself from "
            "dining-related aspects of this case."
        ),
        has_oral_argument=True,
        oral_argument_audio_url=AUDIO_URLS[4],
    ),
    DemoCase(
        docket=_docket(2024, 9),
        case_name="Cicada v. Cricket",
        plaintiff="Cecilia Cicada",
        defendant="Chris Cricket",
        date_filed=date(2024, 5, 15),
        case_type="Noise Complaint",
        status="Closed",
        judge="Hon. Mantis Green",
        summary=(
            "A he-said-she-chirped dispute over nighttime noise levels. "
            "Plaintiff emerges every 17 years only to find defendant "
            "has been making a racket the entire time."
        ),
        has_opinion=True,
        opinion_image_url=IMAGE_URLS[9],
    ),
    DemoCase(
        docket=_docket(2024, 10),
        case_name="Termite v. Carpenter Ant",
        plaintiff="Terry Termite",
        defendant="Carla Carpenter Ant",
        date_filed=date(2024, 6, 1),
        case_type="Unfair Competition",
        status="Pending",
        judge="Hon. Bombardier Burns",
        summary=(
            "Plaintiff alleges defendant is undercutting wood-processing "
            "rates by 40%%, constituting predatory pricing in the "
            "structural-demolition market."
        ),
        has_oral_argument=True,
        oral_argument_audio_url=AUDIO_URLS[5],
    ),

    # ── 2025 ─────────────────────────────────────────────────────
    DemoCase(
        docket=_docket(2025, 1),
        case_name="Dung Beetle v. Fly",
        plaintiff="Douglas Dung Beetle",
        defendant="Francine Fly",
        date_filed=date(2025, 1, 10),
        case_type="Theft",
        status="Closed",
        judge="Hon. Mantis Green",
        summary=(
            "Plaintiff alleges defendant stole his prized dung ball "
            "collection, valued at twelve acorns. Defendant claims "
            "she was merely 'browsing.'"
        ),
        has_opinion=True,
        opinion_image_url=IMAGE_URLS[0],
        has_oral_argument=True,
        oral_argument_audio_url=AUDIO_URLS[6],
    ),
    DemoCase(
        docket=_docket(2025, 2),
        case_name="Praying Mantis v. Cockroach",
        plaintiff="Patricia Praying Mantis",
        defendant="Rocky Roach",
        date_filed=date(2025, 1, 25),
        case_type="Personal Injury",
        status="Pending",
        judge="Hon. Dragonfly Swift",
        summary=(
            "Plaintiff claims injuries sustained from defendant's "
            "sudden appearance in her kitchen at 3 AM. Defense argues "
            "the kitchen is a public thoroughfare."
        ),
    ),
    DemoCase(
        docket=_docket(2025, 3),
        case_name="Flea v. Dog Tick (Much Ado About Biting)",
        plaintiff="Felix Flea",
        defendant="Tina Dog Tick",
        date_filed=date(2025, 2, 14),
        case_type="Territorial Dispute",
        status="Closed",
        judge="Hon. Cricket Chirp",
        summary=(
            "A biting dispute over host-mammal territory. Both parties "
            "claim exclusive biting rights to the neighborhood golden "
            "retriever. Expert witness: the dog, who declined to testify."
        ),
        has_opinion=True,
        opinion_image_url=IMAGE_URLS[1],
        has_oral_argument=True,
        oral_argument_audio_url=AUDIO_URLS[7],
    ),
    DemoCase(
        docket=_docket(2025, 4),
        case_name="Monarch Butterfly v. Viceroy Butterfly",
        plaintiff="Monique Monarch",
        defendant="Victor Viceroy",
        date_filed=date(2025, 3, 1),
        case_type="Trade Dress Infringement",
        status="Pending",
        judge="Hon. Luna Moth",
        summary=(
            "Plaintiff alleges defendant has been copying her iconic "
            "orange-and-black wing pattern for competitive advantage "
            "in the Mullerian mimicry marketplace."
        ),
    ),
    DemoCase(
        docket=_docket(2025, 5),
        case_name="Bee Swarm LLC v. Hornet Inc.",
        plaintiff="Bee Swarm LLC",
        defendant="Hornet Inc.",
        date_filed=date(2025, 3, 20),
        case_type="Corporate Dissolution",
        status="Closed",
        judge="Hon. Silkworm Weaver",
        summary=(
            "A messy corporate divorce. The queen shareholders of "
            "Bee Swarm allege Hornet Inc. engaged in hostile takeover "
            "tactics, literally raiding the corporate hive."
        ),
        has_opinion=True,
        opinion_image_url=IMAGE_URLS[10],
    ),
    DemoCase(
        docket=_docket(2025, 6),
        case_name="Earwig v. Silverfish",
        plaintiff="Edgar Earwig",
        defendant="Silvia Silverfish",
        date_filed=date(2025, 4, 5),
        case_type="Property Damage",
        status="Pending",
        judge="Hon. Bombardier Burns",
        summary=(
            "Plaintiff alleges defendant consumed his rare first-edition "
            "copy of *The Metamorphosis* by Franz Kafka, causing "
            "irreparable literary and emotional damage."
        ),
        has_oral_argument=True,
        oral_argument_audio_url=AUDIO_URLS[8],
    ),
    DemoCase(
        docket=_docket(2025, 7),
        case_name="Weevil v. Grain Moth",
        plaintiff="Walter Weevil",
        defendant="Greta Grain Moth",
        date_filed=date(2025, 4, 22),
        case_type="Breach of Lease",
        status="Closed",
        judge="Hon. Mantis Green",
        summary=(
            "Plaintiff alleges defendant violated the terms of their "
            "shared silo lease by producing larvae that consumed 60%% "
            "of the wheat reserves."
        ),
        has_opinion=True,
        opinion_image_url=IMAGE_URLS[5],
        has_oral_argument=True,
        oral_argument_audio_url=AUDIO_URLS[9],
    ),
    DemoCase(
        docket=_docket(2025, 8),
        case_name="Stink Bug v. Ladybug",
        plaintiff="Stanley Stink Bug",
        defendant="Lucy Ladybug",
        date_filed=date(2025, 5, 10),
        case_type="Defamation",
        status="Pending",
        judge="Hon. Katydid Kafka",
        summary=(
            "Plaintiff claims defendant publicly described his natural "
            "defense mechanism as 'malodorous slander.' Defendant argues "
            "truth is an absolute defense."
        ),
    ),
    DemoCase(
        docket=_docket(2025, 9),
        case_name="Walking Stick v. Leaf Bug",
        plaintiff="Wallace Walking Stick",
        defendant="Liam Leaf Bug",
        date_filed=date(2025, 6, 1),
        case_type="Fraud",
        status="Closed",
        judge="Hon. Cricket Chirp",
        summary=(
            "Both parties are accused of impersonating foliage. "
            "Plaintiff claims his twig-mimicry was first, making "
            "defendant's leaf-mimicry a derivative work."
        ),
        has_opinion=True,
        opinion_image_url=IMAGE_URLS[4],
    ),
    DemoCase(
        docket=_docket(2025, 10),
        case_name="Atlas Moth v. Emperor Moth",
        plaintiff="Atlas Moth",
        defendant="Emperor Moth",
        date_filed=date(2025, 6, 20),
        case_type="Inheritance Dispute",
        status="Pending",
        judge="Hon. Dragonfly Swift",
        summary=(
            "A probate dispute over the late Grand Moth's silk estate. "
            "Plaintiff, the largest moth in the world, argues wingspan "
            "should determine inheritance share."
        ),
        has_oral_argument=True,
        oral_argument_audio_url=AUDIO_URLS[10],
    ),

    # ── 2026 ─────────────────────────────────────────────────────
    DemoCase(
        docket=_docket(2026, 1),
        case_name="Fire Ant v. Carpenter Ant",
        plaintiff="Fernando Fire Ant",
        defendant="Carla Carpenter Ant",
        date_filed=date(2026, 1, 5),
        case_type="Arson",
        status="Pending",
        judge="Hon. Bombardier Burns",
        summary=(
            "Plaintiff is accused of setting fire to defendant's "
            "wooden mound structure. Defense argues his client's name "
            "is merely nominal and not indicative of pyromania."
        ),
        has_oral_argument=True,
        oral_argument_audio_url=AUDIO_URLS[11],
    ),
    DemoCase(
        docket=_docket(2026, 2),
        case_name="Locust v. Grasshopper",
        plaintiff="The Locust Collective",
        defendant="Garrett Grasshopper",
        date_filed=date(2026, 1, 15),
        case_type="Class Action",
        status="Pending",
        judge="Hon. Mantis Green",
        summary=(
            "A class action by the Locust Collective asserting their "
            "right to swarm. Defendant, a solitary grasshopper, argues "
            "that phase polyphenism is a choice, not a right."
        ),
    ),
    DemoCase(
        docket=_docket(2026, 3),
        case_name="Pill Bug v. Centipede",
        plaintiff="Polly Pill Bug",
        defendant="Cecil Centipede",
        date_filed=date(2026, 1, 28),
        case_type="Excessive Force",
        status="Closed",
        judge="Hon. Dragonfly Swift",
        summary=(
            "Plaintiff alleges defendant used excessive force "
            "(specifically, 42 of his 100 legs) during a routine "
            "neighborhood dispute. Plaintiff curled into a ball."
        ),
        has_opinion=True,
        opinion_image_url=IMAGE_URLS[7],
        has_oral_argument=True,
        oral_argument_audio_url=AUDIO_URLS[0],
    ),
    DemoCase(
        docket=_docket(2026, 4),
        case_name="Honeybee v. Cuckoo Bee",
        plaintiff="Helen Honeybee",
        defendant="Colette Cuckoo Bee",
        date_filed=date(2026, 2, 5),
        case_type="Brood Parasitism",
        status="Pending",
        judge="Hon. Silkworm Weaver",
        summary=(
            "Plaintiff discovered defendant had secretly laid eggs in "
            "her hive, a practice known in entomological law as "
            "'brood parasitism in the first degree.'"
        ),
    ),
    DemoCase(
        docket=_docket(2026, 5),
        case_name="Mole Cricket v. Earthworm",
        plaintiff="Morton Mole Cricket",
        defendant="Earl Earthworm",
        date_filed=date(2026, 2, 14),
        case_type="Easement Dispute",
        status="Closed",
        judge="Hon. Cricket Chirp",
        summary=(
            "A subterranean property dispute over tunneling rights. "
            "Plaintiff claims prescriptive easement; defendant argues "
            "he was there first by at least 400 million years."
        ),
        has_opinion=True,
        opinion_image_url=IMAGE_URLS[6],
    ),
    DemoCase(
        docket=_docket(2026, 6),
        case_name="Cicada Emergence LLC v. Periodical Partners",
        plaintiff="Cicada Emergence LLC",
        defendant="Periodical Partners LP",
        date_filed=date(2026, 3, 1),
        case_type="Breach of Contract",
        status="Pending",
        judge="Hon. Katydid Kafka",
        summary=(
            "Plaintiff alleges defendant breached their 17-year "
            "partnership agreement by emerging two years early, "
            "causing market saturation and decreased mate-finding "
            "efficiency."
        ),
        has_oral_argument=True,
        oral_argument_audio_url=AUDIO_URLS[1],
    ),
    DemoCase(
        docket=_docket(2026, 7),
        case_name="Glow Worm v. Firefly",
        plaintiff="Gloria Glow Worm",
        defendant="Flash Firefly",
        date_filed=date(2026, 3, 15),
        case_type="Patent Infringement",
        status="Pending",
        judge="Hon. Luna Moth",
        summary=(
            "Plaintiff holds Patent No. BIO-2019-LUX on 'ground-level "
            "bioluminescent attraction display.' Defendant counters "
            "that aerial bioluminescence is a distinct art."
        ),
    ),
    DemoCase(
        docket=_docket(2026, 8),
        case_name="Assassin Bug v. Bed Bug",
        plaintiff="Artemis Assassin Bug",
        defendant="Bernard Bed Bug",
        date_filed=date(2026, 4, 1),
        case_type="Professional Negligence",
        status="Closed",
        judge="Hon. Bombardier Burns",
        summary=(
            "Plaintiff, a licensed pest-control predator, was hired to "
            "eliminate defendant from a hotel but accidentally destroyed "
            "the minibar. Both parties seek damages."
        ),
        has_opinion=True,
        opinion_image_url=IMAGE_URLS[8],
        has_oral_argument=True,
        oral_argument_audio_url=AUDIO_URLS[2],
    ),
    DemoCase(
        docket=_docket(2026, 9),
        case_name="Jewel Beetle v. Magpie Moth",
        plaintiff="Jasper Jewel Beetle",
        defendant="Magda Magpie Moth",
        date_filed=date(2026, 4, 15),
        case_type="Theft",
        status="Pending",
        judge="Hon. Mantis Green",
        summary=(
            "Plaintiff alleges defendant stole his iridescent elytra "
            "for use in her personal collection. Defense: 'I was "
            "merely attracted to the shiny.'"
        ),
    ),
    DemoCase(
        docket=_docket(2026, 10),
        case_name="Bombardier Beetle v. Blister Beetle",
        plaintiff="Boris Bombardier Beetle",
        defendant="Betty Blister Beetle",
        date_filed=date(2026, 5, 1),
        case_type="Chemical Weapons Violation",
        status="Pending",
        judge="Hon. Mantis Green",
        summary=(
            "Plaintiff accuses defendant of deploying cantharidin in a "
            "residential zone, violating the Geneva Conventions of the "
            "Garden. Defendant accuses plaintiff of hypocrisy, citing "
            "his own boiling-chemical defense mechanism."
        ),
        has_oral_argument=True,
        oral_argument_audio_url=AUDIO_URLS[3],
    ),
]
# fmt: on


# ── Convenience lookups ─────────────────────────────────────────────


def get_case(year: int, number: int) -> DemoCase | None:
    """Look up a case by year and number."""
    docket = _docket(year, number)
    return CASES_BY_DOCKET.get(docket)


def get_justice(slug: str) -> Justice | None:
    """Look up a justice by URL slug."""
    return JUSTICES_BY_SLUG.get(slug)


CASES_BY_DOCKET: dict[str, DemoCase] = {c.docket: c for c in CASES}
JUSTICES_BY_SLUG: dict[str, Justice] = {j.slug: j for j in JUSTICES}
