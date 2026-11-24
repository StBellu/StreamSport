"""Canonical broadcast channel taxonomy, country silos, tag ranking, and flag detection."""

from __future__ import annotations

from functools import lru_cache
import re
from typing import Any, Optional

TVVOO_COUNTRY_GROUPS: tuple[str, ...] = (
    "Italy",
    "Spain",
    "Germany",
    "France",
    "United Kingdom",
)

COUNTRY_SUFFIX_MAP: dict[str, str] = {
    "it": "Italy",
    "italy": "Italy",
    "italia": "Italy",
    "de": "Germany",
    "germany": "Germany",
    "deutschland": "Germany",
    "es": "Spain",
    "spain": "Spain",
    "espana": "Spain",
    "fr": "France",
    "france": "France",
    "uk": "United Kingdom",
    "gb": "United Kingdom",
}

UNSUPPORTED_COUNTRY_SUFFIXES: frozenset[str] = frozenset({
    "ca", "canada",
    "us", "usa",
    "br", "brazil", "brasil",
    "nl", "netherlands",
    "pl", "poland", "polska",
    "ro", "romania",
    "hr", "croatia",
    "rs", "serbia",
    "ba", "bih", "bosnia",
    "me", "mne", "montenegro",
    "si", "slovenia",
    "mk", "mkd", "macedonia",
    "gr", "greece",
    "cy", "cyprus",
    "il", "israel",
    "bg", "bulgaria",
    "al", "albania",
    "az", "azerbaijan",
    "pt", "portugal",
    "dk", "denmark",
    "se", "sweden",
    "no", "norway",
    "cz", "czech",
    "sk", "slovakia",
    "hu", "hungary",
    "tr", "turkey",
    "ru", "russia",
    "ua", "ukraine",
    "ar", "argentina",
    "mx", "mexico",
    "au", "australia",
    "nz", "new zealand",
    "za", "south africa",
})

# Direct mapping for Sky Italia Guida TV API (apid.sky.it) channel IDs
SKY_CHANNEL_IDS: dict[str, str] = {
    "9094": "sky sport 24",       # LCN 200
    "9097": "sky sport uno",      # LCN 201
    "9113": "sky sport calcio",   # LCN 202
    "11237": "sky sport tennis",  # LCN 203
    "9093": "sky sport arena",    # LCN 204
    "9116": "sky sport basket",   # LCN 205
    "9103": "sky sport max",      # LCN 206
    "9096": "sky sport f1",       # LCN 207
    "9102": "sky sport motogp",   # LCN 208
    "10254": "sky sport golf",    # LCN 209
    "11910": "sky sport legend",  # LCN 210
    "11909": "sky sport mix",     # LCN 211
    "6000": "supertennis",        # LCN 212
    "10013": "sky sport 4k",      # LCN 213
    "11402": "dazn 1",            # LCN 214
    "11401": "dazn 2",            # LCN 215
    "11849": "dazn 3",            # LCN 216
    "11850": "dazn 4",            # LCN 217
    "807": "rai sport",           # LCN 227
    "7587": "sport tv",           # LCN 228
    "9513": "milan tv",           # LCN 230
    "9893": "inter tv",           # LCN 232
    "11497": "sky sport 251",     # LCN 251
    "11496": "sky sport 252",     # LCN 252
    "11494": "sky sport 253",     # LCN 253
    "11490": "sky sport 254",     # LCN 254
    "11491": "sky sport 255",     # LCN 255
    "11492": "sky sport 256",     # LCN 256
    "11386": "sky sport 257",     # LCN 257
    "9046": "sky sport 258",      # LCN 258
    "8613": "sky sport 259",      # LCN 259
    "899": "rai 1",               # LCN 101
    "898": "rai 2",               # LCN 102
    "897": "rai 3",               # LCN 103
    "8195": "tv8",                # LCN 125
    "10354": "canale 5",          # LCN 105
    "10454": "italia 1",          # LCN 106
    "12116": "nove",              # LCN 149
    "10458": "20 mediaset",       # LCN 151
    "8133": "cielo",              # LCN 126
    "12120": "dmax",              # LCN 170
    "8293": "equtv",              # LCN 220
}

# MandraKodi FreeShot / Sky A1A159 JIT channel IDs -> canonical keys
SKY_CHID_TO_CANONICAL: dict[str, str] = {
    "skysport24": "sky sport 24",
    "skysportuno": "sky sport uno",
    "skysporttennis": "sky sport tennis",
    "skysportarena": "sky sport arena",
    "skysportbasket": "sky sport basket",
    "skysportmax": "sky sport max",
    "skysportf1": "sky sport f1",
    "skysportmotogp": "sky sport motogp",
    "skysportgolf": "sky sport golf",
    "skysportlegend": "sky sport legend",
    "skysportmix": "sky sport mix",
    "skysport4k": "sky sport 4k",
    "skysportcalcio": "sky sport calcio",
    "skysport251": "sky sport 251",
    "skysport252": "sky sport 252",
    "skysport253": "sky sport 253",
    "skysport254": "sky sport 254",
    "skysport255": "sky sport 255",
    "skysport256": "sky sport 256",
    "skysport257": "sky sport 257",
    "skysport258": "sky sport 258",
    "skysport259": "sky sport 259",
}

COUNTRY_ALIASES: dict[str, dict[str, str]] = {
    "Italy": {
        # Sky Sport 24 (LCN 200)
        "sky sport 24": "sky sport 24",
        "sky sports 24": "sky sport 24",
        "sky sport24": "sky sport 24",
        "sport 24": "sky sport 24",
        "ss24": "sky sport 24",
        "sky sport 200": "sky sport 24",
        "sky 200": "sky sport 24",
        "200": "sky sport 24",

        # Sky Sport Uno (LCN 201)
        "sky sport uno": "sky sport uno",
        "sky sport 1": "sky sport uno",
        "sky 1": "sky sport uno",
        "sport uno": "sky sport uno",
        "sky sport 201": "sky sport uno",
        "sky 201": "sky sport uno",
        "201": "sky sport uno",

        # Sky Sport Calcio (LCN 202)
        "sky sport calcio": "sky sport calcio",
        "sky calcio": "sky sport calcio",
        "sky calcio 1": "sky sport calcio",
        "sport calcio": "sky sport calcio",
        "sky sport 202": "sky sport calcio",
        "sky 202": "sky sport calcio",
        "202": "sky sport calcio",

        # Sky Sport Tennis (LCN 203)
        "sky sport tennis": "sky sport tennis",
        "sky tennis": "sky sport tennis",
        "sport tennis": "sky sport tennis",
        "sky sport 203": "sky sport tennis",
        "sky 203": "sky sport tennis",
        "203": "sky sport tennis",

        # Sky Sport Arena (LCN 204)
        "sky sport arena": "sky sport arena",
        "sky arena": "sky sport arena",
        "sport arena": "sky sport arena",
        "sky sport 204": "sky sport arena",
        "sky 204": "sky sport arena",
        "204": "sky sport arena",

        # Sky Sport Basket (LCN 205, ex Sky Sport NBA)
        "sky sport basket": "sky sport basket",
        "sky basket": "sky sport basket",
        "sky sport nba": "sky sport basket",
        "sky nba": "sky sport basket",
        "sky sport 205": "sky sport basket",
        "sky 205": "sky sport basket",
        "205": "sky sport basket",

        # Sky Sport Max (LCN 206)
        "sky sport max": "sky sport max",
        "sky max": "sky sport max",
        "sport max": "sky sport max",
        "sky sport 206": "sky sport max",
        "sky 206": "sky sport max",
        "206": "sky sport max",

        # Sky Sport F1 (LCN 207)
        "sky sport f1": "sky sport f1",
        "sky f1": "sky sport f1",
        "sky sport 207": "sky sport f1",
        "sky 207": "sky sport f1",
        "207": "sky sport f1",

        # Sky Sport MotoGP (LCN 208)
        "sky sport motogp": "sky sport motogp",
        "sky sport moto gp": "sky sport motogp",
        "sky motogp": "sky sport motogp",
        "sky sport 208": "sky sport motogp",
        "sky 208": "sky sport motogp",
        "208": "sky sport motogp",

        # Sky Sport Golf (LCN 209)
        "sky sport golf": "sky sport golf",
        "sky golf": "sky sport golf",
        "sky sport 209": "sky sport golf",
        "sky 209": "sky sport golf",
        "209": "sky sport golf",

        # Sky Sport Serie A (Legacy) & Event Feed
        "sky sport serie a": "sky sport serie a",
        "sky sport": "sky sport",
        "sky sport it": "sky sport",
        "sky sport eventi": "sky sport",

        # Eurosport
        "eurosport": "eurosport 1",
        "eurosport 1": "eurosport 1",
        "eurosport 210": "eurosport 1",
        "eurosport 2": "eurosport 2",
        "eurosport 211": "eurosport 2",

        # SuperTennis (LCN 212) & OTT Interactive Feeds (isolated from linear H24)
        "supertennis": "supertennis",
        "super tennis": "supertennis",
        "sky super tennis": "supertennis",
        "supertennis 212": "supertennis",
        "sky 212": "supertennis",
        "sky sport 212": "supertennis",
        "212": "supertennis",
        "supertennis 1": "supertennis 1",
        "super tennis 1": "supertennis 1",
        "supertennis 2": "supertennis 2",
        "super tennis 2": "supertennis 2",
        "supertennis 3": "supertennis 3",
        "super tennis 3": "supertennis 3",
        "supertennis 4": "supertennis 4",
        "super tennis 4": "supertennis 4",

        # DAZN Italia (LCN 214-217)
        "dazn 1": "dazn 1",
        "dazn1": "dazn 1",
        "dazn 1 it": "dazn 1",
        "zona dazn": "dazn 1",
        "zona dazn it": "dazn 1",
        "dazn zona": "dazn 1",
        "dazn zona it": "dazn 1",
        "zona dazn 214": "dazn 1",
        "dazn 214": "dazn 1",
        "sky 214": "dazn 1",
        "sky sport 214": "dazn 1",
        "214": "dazn 1",
        "dazn 2": "dazn 2",
        "dazn2": "dazn 2",
        "dazn 2 it": "dazn 2",
        "dazn 2 italia": "dazn 2",
        "zona dazn 2": "dazn 2",
        "zona dazn 2 it": "dazn 2",
        "dazn zona 2": "dazn 2",
        "dazn zona 2 it": "dazn 2",
        "zona dazn 215": "dazn 2",
        "dazn 215": "dazn 2",
        "sky 215": "dazn 2",
        "sky sport 215": "dazn 2",
        "215": "dazn 2",
        "dazn 3": "dazn 3",
        "dazn3": "dazn 3",
        "dazn 3 it": "dazn 3",
        "dazn 3 italia": "dazn 3",
        "zona dazn 3": "dazn 3",
        "zona dazn 216": "dazn 3",
        "dazn 216": "dazn 3",
        "sky 216": "dazn 3",
        "sky sport 216": "dazn 3",
        "216": "dazn 3",
        "dazn 4": "dazn 4",
        "dazn4": "dazn 4",
        "dazn 4 it": "dazn 4",
        "dazn 4 italia": "dazn 4",
        "zona dazn 4": "dazn 4",
        "zona dazn 217": "dazn 4",
        "dazn 217": "dazn 4",
        "sky 217": "dazn 4",
        "sky sport 217": "dazn 4",
        "217": "dazn 4",

        # Sky Sport Legend (210), Mix (211), 4K (213)
        "sky sport legend": "sky sport legend",
        "sky legend": "sky sport legend",
        "sky sport 210": "sky sport legend",
        "sky 210": "sky sport legend",
        "210": "sky sport legend",
        "sky sport mix": "sky sport mix",
        "sky mix": "sky sport mix",
        "sky sport mix 211": "sky sport mix",
        "sky sport 211": "sky sport mix",
        "sky 211": "sky sport mix",
        "211": "sky sport mix",
        "sky sport 4k": "sky sport 4k",
        "sky 4k": "sky sport 4k",
        "sky sport 213": "sky sport 4k",
        "sky 213": "sky sport 4k",
        "213": "sky sport 4k",

        # Sky Sport Calcio Contemporanea (LCN 251-259)
        "sky sport 251": "sky sport 251",
        "sky sport calcio 251": "sky sport 251",
        "sky calcio 251": "sky sport 251",
        "sky 251": "sky sport 251",
        "251": "sky sport 251",
        "sky sport 252": "sky sport 252",
        "sky sport calcio 252": "sky sport 252",
        "sky calcio 252": "sky sport 252",
        "sky calcio 2": "sky sport 252",
        "sky 252": "sky sport 252",
        "252": "sky sport 252",
        "sky sport 253": "sky sport 253",
        "sky sport calcio 253": "sky sport 253",
        "sky calcio 253": "sky sport 253",
        "sky calcio 3": "sky sport 253",
        "sky 253": "sky sport 253",
        "253": "sky sport 253",
        "sky sport 254": "sky sport 254",
        "sky sport calcio 254": "sky sport 254",
        "sky calcio 254": "sky sport 254",
        "sky calcio 4": "sky sport 254",
        "sky 254": "sky sport 254",
        "254": "sky sport 254",
        "sky sport 255": "sky sport 255",
        "sky sport calcio 255": "sky sport 255",
        "sky calcio 255": "sky sport 255",
        "sky calcio 5": "sky sport 255",
        "sky 255": "sky sport 255",
        "255": "sky sport 255",
        "sky sport 256": "sky sport 256",
        "sky sport calcio 256": "sky sport 256",
        "sky calcio 256": "sky sport 256",
        "sky calcio 6": "sky sport 256",
        "sky 256": "sky sport 256",
        "256": "sky sport 256",
        "sky sport 257": "sky sport 257",
        "sky sport calcio 257": "sky sport 257",
        "sky calcio 257": "sky sport 257",
        "sky calcio 7": "sky sport 257",
        "sky 257": "sky sport 257",
        "257": "sky sport 257",
        "sky sport 258": "sky sport 258",
        "sky sport calcio 258": "sky sport 258",
        "sky calcio 258": "sky sport 258",
        "sky calcio 8": "sky sport 258",
        "sky 258": "sky sport 258",
        "258": "sky sport 258",
        "sky sport 259": "sky sport 259",
        "sky sport calcio 259": "sky sport 259",
        "sky calcio 259": "sky sport 259",
        "sky calcio 9": "sky sport 259",
        "sky 259": "sky sport 259",
        "259": "sky sport 259",

        # Club channels & FTA Italian
        "sport tv": "sport tv",
        "sport tv 228": "sport tv",
        "sky 228": "sport tv",
        "228": "sport tv",
        "milan tv": "milan tv",
        "milan tv 230": "milan tv",
        "sky 230": "milan tv",
        "230": "milan tv",
        "inter tv": "inter tv",
        "inter tv 232": "inter tv",
        "sky 232": "inter tv",
        "232": "inter tv",
        "20 mediaset": "20 mediaset",
        "canale 20": "20 mediaset",
        "mediaset 20": "20 mediaset",
        "venti": "20 mediaset",
        "20": "20 mediaset",
        "rai 4k": "rai 4k",
        "rai sport": "rai sport",
        "raisport": "rai sport",
        "rai sport +": "rai sport",
        "rai sport+": "rai sport",
        "rai sport 1": "rai sport",
        "sportitalia": "sportitalia",
        "sportitalia 60": "sportitalia",
        "sportitalia plus": "sportitalia plus",
        "sportitalia solocalcio": "sportitalia solocalcio",
        "sportitalia solo calcio": "sportitalia solocalcio",
        "solocalcio": "sportitalia solocalcio",
        "solo calcio": "sportitalia solocalcio",
        "tv8": "tv8",
        "tv 8": "tv8",
        "cielo": "cielo",
        "canale 5": "canale 5",
        "canale5": "canale 5",
        "italia 1": "italia 1",
        "italia1": "italia 1",
        "rete 4": "rete 4",
        "rete4": "rete 4",
        "nove": "nove",
        "nove tv": "nove",
        "canale nove": "nove",
        "il nove": "nove",
        "equtv": "equtv",
        "equ tv": "equtv",
        "dmax": "dmax",
        "rai 1": "rai 1",
        "rai1": "rai 1",
        "rai 2": "rai 2",
        "rai2": "rai 2",
        "rai 3": "rai 3",
        "rai3": "rai 3",
        "prime video": "prime video it",
        "prime video it": "prime video it",
        "prime video italia": "prime video it",
        "amazon prime": "prime video it",
        "amazon prime video": "prime video it",
        "amazon prime video it": "prime video it",
        "amazon prime video italia": "prime video it",
        "amazon prime sport": "prime video it",
    },
    "Germany": {
        "magenta sport 1": "magenta sport 1",
        "magenta sport 301": "magenta sport 1",
        "301": "magenta sport 1",
        "magenta sport 2": "magenta sport 2",
        "magenta sport 302": "magenta sport 2",
        "302": "magenta sport 2",
        "magenta sport 3": "magenta sport 3",
        "magenta sport 303": "magenta sport 3",
        "303": "magenta sport 3",
        "magenta sport 4": "magenta sport 4",
        "magenta sport 304": "magenta sport 4",
        "304": "magenta sport 4",
        "magenta sport 5": "magenta sport 5",
        "magenta sport 305": "magenta sport 5",
        "305": "magenta sport 5",
        "magenta sport 6": "magenta sport 6",
        "magenta sport 306": "magenta sport 6",
        "306": "magenta sport 6",
        "magenta sport 7": "magenta sport 7",
        "magenta sport 307": "magenta sport 7",
        "307": "magenta sport 7",
        "magenta sport 8": "magenta sport 8",
        "magenta sport 308": "magenta sport 8",
        "308": "magenta sport 8",
        "magenta sport 9": "magenta sport 9",
        "magenta sport 309": "magenta sport 9",
        "309": "magenta sport 9",
        "magenta sport 10": "magenta sport 10",
        "magenta sport 310": "magenta sport 10",
        "310": "magenta sport 10",
        "sky sport bundesliga": "sky sport bundesliga",
        "sky bundesliga": "sky sport bundesliga",
        "sky sport bundesliga 1": "sky sport bundesliga 1",
        "sky sport bundesliga 2": "sky sport bundesliga 2",
        "sky sport bundesliga 3": "sky sport bundesliga 3",
        "sky sport bundesliga 4": "sky sport bundesliga 4",
        "sky sport bundesliga 5": "sky sport bundesliga 5",
        "sky sport bundesliga 6": "sky sport bundesliga 6",
        "sky sport bundesliga 7": "sky sport bundesliga 7",
        "sky sport bundesliga 8": "sky sport bundesliga 8",
        "sky sport bundesliga 9": "sky sport bundesliga 9",
        "sky sport bundesliga 10": "sky sport bundesliga 10",
        "sky sport top event": "sky sport top event",
        "sky top event": "sky sport top event",
        "sky sport premier league": "sky sport premier league de",
        "sky sport premier league de": "sky sport premier league de",
        "sky premier league de": "sky sport premier league de",
        "sky sport 1": "sky sport 1 de",
        "sky sport 1 de": "sky sport 1 de",
        "sky sport 2": "sky sport 2 de",
        "sky sport 2 de": "sky sport 2 de",
        "sky sport 3": "sky sport 3 de",
        "sky sport 3 de": "sky sport 3 de",
        "sky sport 4": "sky sport 4 de",
        "sky sport 4 de": "sky sport 4 de",
        "sky sport 5": "sky sport 5 de",
        "sky sport 5 de": "sky sport 5 de",
        "sky sport 6": "sky sport 6 de",
        "sky sport 6 de": "sky sport 6 de",
        "sky sport 7": "sky sport 7 de",
        "sky sport 7 de": "sky sport 7 de",
        "sky sport 8": "sky sport 8 de",
        "sky sport 8 de": "sky sport 8 de",
        "sky sport 9": "sky sport 9 de",
        "sky sport 9 de": "sky sport 9 de",
        "sky sport 10": "sky sport 10 de",
        "sky sport 10 de": "sky sport 10 de",
        "sky sport 11": "sky sport 11 de",
        "sky sport 11 de": "sky sport 11 de",
        "sky sport f1": "sky sport f1 de",
        "sky sport f1 de": "sky sport f1 de",
        "sky sport f1 germany": "sky sport f1 de",
        "sky sport tennis": "sky sport tennis de",
        "sky sport tennis de": "sky sport tennis de",
        "sky sport golf": "sky sport golf de",
        "sky sport golf de": "sky sport golf de",
        "sky sport mix": "sky sport mix de",
        "sky sport mix de": "sky sport mix de",
        "dazn 1": "dazn 1 de",
        "dazn 1 de": "dazn 1 de",
        "dazn 1 germany": "dazn 1 de",
        "dazn 2": "dazn 2 de",
        "dazn 2 de": "dazn 2 de",
        "dazn 2 germany": "dazn 2 de",
        "sport1": "sport1 de",
        "sport 1": "sport1 de",
        "sport1 de": "sport1 de",
        "sport 1 de": "sport1 de",
        "sport1 germany": "sport1 de",
        "amazon prime sport": "amazon prime sport de",
        "amazon pr me sport": "amazon prime sport de",
        "amazon prime": "amazon prime de",
        "amazon prime de": "amazon prime de",
        "amazon prime video": "amazon prime de",
        "amazon prime video 1": "amazon prime video 1 de",
        "amazon prime video 2": "amazon prime video 2 de",
        "germany box amazon prime": "amazon prime de",
        "dazn nba": "dazn nba de",
    },
    "Spain": {
        "movistar laliga": "movistar laliga",
        "movistar laliga 1": "movistar laliga 1",
        "movistar laliga 2": "movistar laliga 2",
        "movistar laliga 3": "movistar laliga 3",
        "movistar laliga 4": "movistar laliga 4",
        "laliga tv por movistar plus": "movistar laliga",
        "movistar liga de campeones": "movistar liga campeones",
        "movistar liga campeones": "movistar liga campeones",
        "movistar campeones": "movistar liga campeones",
        "movistar liga campeones 1": "movistar liga campeones 1",
        "movistar liga campeones 2": "movistar liga campeones 2",
        "movistar liga campeones 3": "movistar liga campeones 3",
        "movistar liga campeones 4": "movistar liga campeones 4",
        "movistar liga campeones 5": "movistar liga campeones 5",
        "movistar liga campeones 6": "movistar liga campeones 6",
        "movistar liga campeones 7": "movistar liga campeones 7",
        "movistar liga campeones 8": "movistar liga campeones 8",
        "movistar liga campeones 9": "movistar liga campeones 9",
        "movistar liga campeones 10": "movistar liga campeones 10",
        "movistar deportes": "movistar deportes",
        "movistar deporte 1": "movistar deportes 1",
        "movistar deportes 1": "movistar deportes 1",
        "movistar deportes 2": "movistar deportes 2",
        "movistar deportes 3": "movistar deportes 3",
        "movistar deportes 4": "movistar deportes 4",
        "movistar deportes 5": "movistar deportes 5",
        "movistar golf": "movistar golf",
        "movistar vamos": "movistar vamos",
        "movistar #vamos": "movistar vamos",
        "dazn 1": "dazn 1 es",
        "dazn 1 es": "dazn 1 es",
        "dazn 1 esp": "dazn 1 es",
        "dazn 1 spain": "dazn 1 es",
        "dazn 2": "dazn 2 es",
        "dazn 2 es": "dazn 2 es",
        "dazn 2 esp": "dazn 2 es",
        "dazn 2 spain": "dazn 2 es",
        "dazn 3": "dazn 3 es",
        "dazn 3 es": "dazn 3 es",
        "dazn 3 spain": "dazn 3 es",
        "dazn 4": "dazn 4 es",
        "dazn 4 es": "dazn 4 es",
        "dazn 4 spain": "dazn 4 es",
        "dazn laliga": "dazn laliga",
        "dazn laliga 1": "dazn laliga 1",
        "dazn laliga 2": "dazn laliga 2",
        "dazn f1": "dazn f1 es",
        "dazn f1 es": "dazn f1 es",
        "gol play": "gol play",
        "gol": "gol play",
        "teledeporte": "teledeporte",
        "tve teledeporte": "teledeporte",
        "bein sports la liga": "bein sports la liga",
        "prime video": "prime video es",
        "prime video es": "prime video es",
        "amazon prime": "prime video es",
        "amazon prime video": "prime video es",
        "amazon prime video es": "prime video es",
    },
    "France": {
        "canal+ sport": "canal plus sport",
        "canal sport": "canal plus sport",
        "canal plus sport": "canal plus sport",
        "canal+ foot": "canal plus foot",
        "canal foot": "canal plus foot",
        "canal plus foot": "canal plus foot",
        "canal+ 360": "canal plus 360",
        "canal 360": "canal plus 360",
        "canal plus 360": "canal plus 360",
        "canal+": "canal plus",
        "canal plus": "canal plus",
        "canal+ live 1": "canal plus live 1",
        "canal plus live 1": "canal plus live 1",
        "canal+ live 2": "canal plus live 2",
        "canal plus live 2": "canal plus live 2",
        "canal+ live 3": "canal plus live 3",
        "canal plus live 3": "canal plus live 3",
        "bein sports 1": "bein sports 1 fr",
        "bein sports 1 fr": "bein sports 1 fr",
        "bein sports 1 france": "bein sports 1 fr",
        "bein sports hd 1 france": "bein sports 1 fr",
        "bein sports 2": "bein sports 2 fr",
        "bein sports 2 fr": "bein sports 2 fr",
        "bein sports 2 france": "bein sports 2 fr",
        "bein sports 3": "bein sports 3 fr",
        "bein sports 3 fr": "bein sports 3 fr",
        "bein sports 3 france": "bein sports 3 fr",
        "bein sports max 4": "bein sports max 4 fr",
        "bein sports max 4 fr": "bein sports max 4 fr",
        "bein sports max 5": "bein sports max 5 fr",
        "bein sports max 5 fr": "bein sports max 5 fr",
        "bein sports max 6": "bein sports max 6 fr",
        "bein sports max 6 fr": "bein sports max 6 fr",
        "bein sports max 7": "bein sports max 7 fr",
        "bein sports max 7 fr": "bein sports max 7 fr",
        "bein sports max 8": "bein sports max 8 fr",
        "bein sports max 8 fr": "bein sports max 8 fr",
        "bein sports max 9": "bein sports max 9 fr",
        "bein sports max 9 fr": "bein sports max 9 fr",
        "bein sports max 10": "bein sports max 10 fr",
        "bein sports max 10 fr": "bein sports max 10 fr",
        "dazn 1": "dazn 1 fr",
        "dazn 1 fr": "dazn 1 fr",
        "dazn 2": "dazn 2 fr",
        "dazn 2 fr": "dazn 2 fr",
        "dazn 3": "dazn 3 fr",
        "dazn 3 fr": "dazn 3 fr",
        "rmc sport 1": "rmc sport 1",
        "rmc sport 2": "rmc sport 2",
        "amazon prime 1": "amazon prime 1 fr",
        "amazon prime 1 +": "amazon prime 1 fr",
        "amazon prime 2": "amazon prime 2 fr",
        "amazon prime 2 +": "amazon prime 2 fr",
        "amazon prime 3": "amazon prime 3 fr",
        "amazon prime 3 +": "amazon prime 3 fr",
        "amazon prime 4": "amazon prime 4 fr",
        "amazon prime 4 +": "amazon prime 4 fr",
        "amazon prime 5": "amazon prime 5 fr",
        "amazon prime 5 +": "amazon prime 5 fr",
        "amazon prime 6": "amazon prime 6 fr",
        "amazon prime 7": "amazon prime 7 fr",
        "amazon prime 8": "amazon prime 8 fr",
        "amazon prime": "amazon prime 1 fr",
        "amazon prime video": "amazon prime 1 fr",
    },
    "United Kingdom": {
        "sky sports main event": "sky sports main event",
        "sky main event": "sky sports main event",
        "sky sports premier league": "sky sports premier league uk",
        "sky sports football": "sky sports football uk",
        "sky sports f1": "sky sports f1 uk",
        "sky sports f1 uk": "sky sports f1 uk",
        "sky sports cricket": "sky sports cricket uk",
        "sky sports action": "sky sports action uk",
        "sky sports arena": "sky sports arena uk",
        "sky sports arena uk": "sky sports arena uk",
        "sky sports golf": "sky sports golf uk",
        "sky sports tennis": "sky sports tennis uk",
        "sky sports news": "sky sports news uk",
        "sky sports mix": "sky sports mix uk",
        "sky sports nfl": "sky sports nfl",
        "tnt sports 1": "tnt sports 1",
        "tnt sport 1": "tnt sports 1",
        "bt sport 1": "tnt sports 1",
        "tnt sports 2": "tnt sports 2",
        "tnt sport 2": "tnt sports 2",
        "bt sport 2": "tnt sports 2",
        "tnt sports 3": "tnt sports 3",
        "tnt sport 3": "tnt sports 3",
        "bt sport 3": "tnt sports 3",
        "tnt sports 4": "tnt sports 4",
        "tnt sport 4": "tnt sports 4",
        "bt sport 4": "tnt sports 4",
        "tnt sports ultimate": "tnt sports ultimate",
        "bbc one": "bbc one",
        "bbc two": "bbc two",
        "itv 1": "itv 1",
        "itv 4": "itv 4",
        "amazon": "amazon prime uk",
        "amazon prime": "amazon prime uk",
        "amazon prime uk": "amazon prime uk",
        "amazon prime video": "amazon prime uk",
        "amazon prime video uk": "amazon prime uk",
        "prime video uk": "amazon prime uk",
    },
    "World": {
        "euroleague tv": "euroleague tv",
        "nba tv": "nba tv",
        "nba tv usa": "nba tv",
        "nba league pass": "nba league pass",
        "nba pass": "nba league pass",
    },
}

# Flat combined aliases for country-agnostic lookups: Italy loaded last so IT wins on shared keys.
GLOBAL_CHANNEL_ALIASES: dict[str, str] = {}
for _group_name in ("World", "United Kingdom", "France", "Spain", "Germany", "Italy"):
    GLOBAL_CHANNEL_ALIASES.update(COUNTRY_ALIASES.get(_group_name, {}))

CANONICAL_DISPLAY_NAMES: dict[str, str] = {
    "sky sport uno": "Sky Sport Uno",
    "sky sport calcio": "Sky Sport Calcio",
    "sky sport tennis": "Sky Sport Tennis",
    "sky sport arena": "Sky Sport Arena",
    "sky sport f1": "Sky Sport F1",
    "sky sport motogp": "Sky Sport MotoGP",
    "sky sport basket": "Sky Sport Basket",
    "sky sport 24": "Sky Sport 24",
    "sky sport golf": "Sky Sport Golf",
    "sky sport max": "Sky Sport Max",
    "sky sport mix": "Sky Sport Mix",
    "sky sport serie a": "Sky Sport Serie A",
    "sky sport": "Sky Sport",
    "dazn 1": "DAZN 1",
    "dazn 2": "DAZN 2",
    "dazn 3": "DAZN 3",
    "dazn 4": "DAZN 4",
    "eurosport 1": "Eurosport 1",
    "eurosport 2": "Eurosport 2",
    "rai sport": "Rai Sport",
    "sportitalia": "Sportitalia",
    "sportitalia plus": "Sportitalia Plus",
    "sportitalia solocalcio": "Sportitalia SoloCalcio",
    "supertennis": "SuperTennis",
    "tv8": "TV8",
    "cielo": "Cielo",
    "canale 5": "Canale 5",
    "italia 1": "Italia 1",
    "rete 4": "Rete 4",
    "nove": "NOVE",
    "equtv": "EQUtv",
    "dmax": "DMAX",
    "rai 1": "Rai 1",
    "rai 2": "Rai 2",
    "rai 3": "Rai 3",
    "rai 4k": "Rai 4K",
    "prime video it": "Prime Video",
    "sky sport 4k": "Sky Sport 4K",
    "sky sport legend": "Sky Sport Legend",
    "sky sport 251": "Sky Sport 251",
    "sky sport 252": "Sky Sport 252",
    "sky sport 253": "Sky Sport 253",
    "sky sport 254": "Sky Sport 254",
    "sky sport 255": "Sky Sport 255",
    "sky sport 256": "Sky Sport 256",
    "sky sport 257": "Sky Sport 257",
    "sky sport 258": "Sky Sport 258",
    "sky sport 259": "Sky Sport 259",
    "sport tv": "Sport TV",
    "milan tv": "Milan TV",
    "inter tv": "Inter TV",
    "20 mediaset": "20 Mediaset",
    "magenta sport 1": "🇩🇪 Magenta Sport 1",
    "magenta sport 2": "🇩🇪 Magenta Sport 2",
    "magenta sport 3": "🇩🇪 Magenta Sport 3",
    "magenta sport 4": "🇩🇪 Magenta Sport 4",
    "magenta sport 5": "🇩🇪 Magenta Sport 5",
    "magenta sport 6": "🇩🇪 Magenta Sport 6",
    "magenta sport 7": "🇩🇪 Magenta Sport 7",
    "magenta sport 8": "🇩🇪 Magenta Sport 8",
    "magenta sport 9": "🇩🇪 Magenta Sport 9",
    "magenta sport 10": "🇩🇪 Magenta Sport 10",
    "sky sport bundesliga": "🇩🇪 Sky Sport Bundesliga",
    "sky sport bundesliga 1": "🇩🇪 Sky Sport Bundesliga 1",
    "sky sport bundesliga 2": "🇩🇪 Sky Sport Bundesliga 2",
    "sky sport bundesliga 3": "🇩🇪 Sky Sport Bundesliga 3",
    "sky sport bundesliga 4": "🇩🇪 Sky Sport Bundesliga 4",
    "sky sport bundesliga 5": "🇩🇪 Sky Sport Bundesliga 5",
    "sky sport top event": "🇩🇪 Sky Sport Top Event",
    "sky sport premier league de": "🇩🇪 Sky Sport Premier League",
    "sky sport 1 de": "🇩🇪 Sky Sport 1 DE",
    "sky sport 2 de": "🇩🇪 Sky Sport 2 DE",
    "sky sport 3 de": "🇩🇪 Sky Sport 3 DE",
    "sky sport 4 de": "🇩🇪 Sky Sport 4 DE",
    "sky sport 5 de": "🇩🇪 Sky Sport 5 DE",
    "sky sport 6 de": "🇩🇪 Sky Sport 6 DE",
    "sky sport 7 de": "🇩🇪 Sky Sport 7 DE",
    "sky sport 8 de": "🇩🇪 Sky Sport 8 DE",
    "sky sport 9 de": "🇩🇪 Sky Sport 9 DE",
    "sky sport 10 de": "🇩🇪 Sky Sport 10 DE",
    "sky sport 11 de": "🇩🇪 Sky Sport 11 DE",
    "sky sport f1 de": "🇩🇪 Sky Sport F1 DE",
    "sky sport tennis de": "🇩🇪 Sky Sport Tennis DE",
    "sky sport golf de": "🇩🇪 Sky Sport Golf DE",
    "sky sport mix de": "🇩🇪 Sky Sport Mix DE",
    "dazn 1 de": "🇩🇪 DAZN 1 DE",
    "dazn 2 de": "🇩🇪 DAZN 2 DE",
    "sport1 de": "🇩🇪 Sport1 DE",
    "amazon prime sport de": "🇩🇪 Amazon Prime Sport",
    "amazon prime de": "🇩🇪 Amazon Prime Video",
    "amazon prime video 1 de": "🇩🇪 Amazon Prime Video 1",
    "amazon prime video 2 de": "🇩🇪 Amazon Prime Video 2",
    "dazn nba de": "🇩🇪 DAZN NBA",
    "movistar laliga": "🇪🇸 Movistar LaLiga",
    "movistar laliga 1": "🇪🇸 Movistar LaLiga 1",
    "movistar laliga 2": "🇪🇸 Movistar LaLiga 2",
    "movistar laliga 3": "🇪🇸 Movistar LaLiga 3",
    "movistar laliga 4": "🇪🇸 Movistar LaLiga 4",
    "movistar liga campeones": "🇪🇸 Movistar Liga de Campeones",
    "movistar liga campeones 1": "🇪🇸 Movistar Liga de Campeones 1",
    "movistar liga campeones 2": "🇪🇸 Movistar Liga de Campeones 2",
    "movistar liga campeones 3": "🇪🇸 Movistar Liga de Campeones 3",
    "movistar deportes": "🇪🇸 Movistar Deportes",
    "movistar deportes 1": "🇪🇸 Movistar Deportes 1",
    "movistar deportes 2": "🇪🇸 Movistar Deportes 2",
    "movistar golf": "🇪🇸 Movistar Golf",
    "movistar vamos": "🇪🇸 Movistar #Vamos",
    "dazn 1 es": "🇪🇸 DAZN 1 ES",
    "dazn 2 es": "🇪🇸 DAZN 2 ES",
    "dazn 3 es": "🇪🇸 DAZN 3 ES",
    "dazn 4 es": "🇪🇸 DAZN 4 ES",
    "dazn laliga": "🇪🇸 DAZN LaLiga",
    "dazn laliga 1": "🇪🇸 DAZN LaLiga 1",
    "dazn laliga 2": "🇪🇸 DAZN LaLiga 2",
    "dazn f1 es": "🇪🇸 DAZN F1 ES",
    "gol play": "🇪🇸 Gol Play",
    "teledeporte": "🇪🇸 Teledeporte",
    "bein sports la liga": "🇪🇸 beIN Sports LaLiga",
    "prime video es": "🇪🇸 Prime Video",
    "canal plus sport": "🇫🇷 Canal+ Sport",
    "canal plus foot": "🇫🇷 Canal+ Foot",
    "canal plus 360": "🇫🇷 Canal+ 360",
    "canal plus": "🇫🇷 Canal+",
    "canal plus live 1": "🇫🇷 Canal+ Live 1",
    "canal plus live 2": "🇫🇷 Canal+ Live 2",
    "bein sports 1 fr": "🇫🇷 beIN Sports 1",
    "bein sports 2 fr": "🇫🇷 beIN Sports 2",
    "bein sports 3 fr": "🇫🇷 beIN Sports 3",
    "bein sports max 4 fr": "🇫🇷 beIN Sports Max 4",
    "bein sports max 5 fr": "🇫🇷 beIN Sports Max 5",
    "bein sports max 6 fr": "🇫🇷 beIN Sports Max 6",
    "dazn 1 fr": "🇫🇷 DAZN 1 FR",
    "dazn 2 fr": "🇫🇷 DAZN 2 FR",
    "dazn 3 fr": "🇫🇷 DAZN 3 FR",
    "rmc sport 1": "🇫🇷 RMC Sport 1",
    "rmc sport 2": "🇫🇷 RMC Sport 2",
    "amazon prime 1 fr": "🇫🇷 Amazon Prime 1",
    "amazon prime 2 fr": "🇫🇷 Amazon Prime 2",
    "amazon prime 3 fr": "🇫🇷 Amazon Prime 3",
    "amazon prime 4 fr": "🇫🇷 Amazon Prime 4",
    "amazon prime 5 fr": "🇫🇷 Amazon Prime 5",
    "amazon prime 6 fr": "🇫🇷 Amazon Prime 6",
    "amazon prime 7 fr": "🇫🇷 Amazon Prime 7",
    "amazon prime 8 fr": "🇫🇷 Amazon Prime 8",
    "sky sports main event": "🇬🇧 Sky Sports Main Event",
    "sky sports premier league uk": "🇬🇧 Sky Sports Premier League",
    "sky sports football uk": "🇬🇧 Sky Sports Football",
    "sky sports f1 uk": "🇬🇧 Sky Sports F1",
    "sky sports cricket uk": "🇬🇧 Sky Sports Cricket",
    "sky sports action uk": "🇬🇧 Sky Sports Action",
    "sky sports arena uk": "🇬🇧 Sky Sports Arena",
    "sky sports golf uk": "🇬🇧 Sky Sports Golf",
    "sky sports tennis uk": "🇬🇧 Sky Sports Tennis",
    "sky sports news uk": "🇬🇧 Sky Sports News",
    "sky sports mix uk": "🇬🇧 Sky Sports Mix",
    "sky sports nfl": "🇬🇧 Sky Sports NFL",
    "tnt sports 1": "🇬🇧 TNT Sports 1",
    "tnt sports 2": "🇬🇧 TNT Sports 2",
    "tnt sports 3": "🇬🇧 TNT Sports 3",
    "tnt sports 4": "🇬🇧 TNT Sports 4",
    "tnt sports ultimate": "🇬🇧 TNT Sports Ultimate",
    "bbc one": "🇬🇧 BBC One",
    "bbc two": "🇬🇧 BBC Two",
    "itv 1": "🇬🇧 ITV 1",
    "itv 4": "🇬🇧 ITV 4",
    "amazon prime uk": "🇬🇧 Amazon Prime Video",
    "euroleague tv": "🌍 EuroLeague TV",
    "nba tv": "🌍 NBA TV",
    "nba league pass": "🌍 NBA League Pass",
}

CHANNEL_COUNTRY_PATTERNS: tuple[tuple[re.Pattern[str], Optional[re.Pattern[str]], str], ...] = (
    (
        re.compile(
            r"\b(it|ita|italy|italia|rai|calcio|raisport|supertennis|sportitalia|"
            r"mediaset|canale\s*5|italia\s*1|rete\s*4|tv8|cielo|dmax|equtv|nove|"
            r"sky\s*sport\s*(?:uno|basket|legend|mix|max|arena|tennis|f1|motogp|golf|24|25[1-9]|4k))\b",
            re.IGNORECASE,
        ),
        re.compile(
            r"\b(uk|gb|spain|espana|es|germany|deutschland|de|austria|at|ca|canada|"
            r"us|usa|turkey|turkiye|tr|croatia|hrvatska|hr|serbia|srbija|rs|"
            r"france|fr|poland|polska|pl|netherlands|holland|nl|russia|ru|pt|portugal)\b",
            re.IGNORECASE,
        ),
        "🇮🇹",
    ),
    (re.compile(r"\b(uk|gb|england|united\s*kingdom|bbc|itv|tnt\s*sports?|bt\s*sport|sky\s*sports)\b", re.IGNORECASE), None, "🇬🇧"),
    (re.compile(r"\b(usa?|espn|fox\s*sports?|cbs|nbc|abc|tnt\s*us|tbs|bally|msg|yes\s*network|nesn|altitude|marquee|fs[12])\b", re.IGNORECASE), None, "🇺🇸"),
    (re.compile(r"\b(es|esp|spain|espana|movistar|laliga\s*tv|gol\s*play|teledeporte)\b", re.IGNORECASE), None, "🇪🇸"),
    (re.compile(r"\b(fr|france|canal\+|canal\s*plus|rmc\s*sport|bein\s*sports?\s*fr)\b", re.IGNORECASE), None, "🇫🇷"),
    (re.compile(r"\b(de|germany|deutschland|austria|zdf|ard|magenta\s*sport|sport1)\b", re.IGNORECASE), None, "🇩🇪"),
    (re.compile(r"\b(ca|canada|tsn|sportsnet)\b", re.IGNORECASE), None, "🇨🇦"),
    (re.compile(r"\b(ru|russia|match!|match\s*tv|match\s*premier|match\s*football|okko)\b", re.IGNORECASE), None, "🇷🇺"),
    (re.compile(r"\b(pl|poland|polska|polsat|canal\+\s*sport\s*pl|tvp\s*sport|eleven\s*sports?\s*pl)\b", re.IGNORECASE), None, "🇵🇱"),
    (re.compile(r"\b(nl|netherlands|holland|ziggo)\b", re.IGNORECASE), None, "🇳🇱"),
    (re.compile(r"\b(pt|portugal|sport\s*tv\s*pt|eleven\s*sports?\s*pt)\b", re.IGNORECASE), None, "🇵🇹"),
    (re.compile(r"\b(br|brazil|brasil|globo|sportv|premiere)\b", re.IGNORECASE), None, "🇧🇷"),
    (re.compile(r"\b(tr|turkey|turkiye|tivibu|s\s*sport|a\s*spor|bein\s*sports?\s*tr)\b", re.IGNORECASE), None, "🇹🇷"),
    (re.compile(r"\b(hr|croatia|hrvatska)\b", re.IGNORECASE), None, "🇭🇷"),
    (re.compile(r"\b(rs|serbia|srbija)\b", re.IGNORECASE), None, "🇷🇸"),
    (re.compile(r"\b(gr|greece|cosmote|novasports)\b", re.IGNORECASE), None, "🇬🇷"),
    (re.compile(r"\b(ro|romania|digi\s*sport|prima\s*sport)\b", re.IGNORECASE), None, "🇷🇴"),
    (re.compile(r"\b(ar|arabic|uae|saudi|ssc|alkass)\b", re.IGNORECASE), None, "🇸🇦"),
    (re.compile(r"\b(au|australia|kayo|stan\s*sport|optus)\b", re.IGNORECASE), None, "🇦🇺"),
)

_LANG_FLAG_MAP: dict[str, str] = {
    "it": "🇮🇹", "ita": "🇮🇹",
    "uk": "🇬🇧", "gb": "🇬🇧", "en": "🇬🇧", "eng": "🇬🇧",
    "us": "🇺🇸", "usa": "🇺🇸",
    "de": "🇩🇪", "deu": "🇩🇪", "ger": "🇩🇪",
    "es": "🇪🇸", "spa": "🇪🇸",
    "fr": "🇫🇷", "fra": "🇫🇷",
    "pt": "🇵🇹", "por": "🇵🇹",
    "nl": "🇳🇱", "nld": "🇳🇱",
    "pl": "🇵🇱", "pol": "🇵🇱",
    "tr": "🇹🇷", "tur": "🇹🇷",
    "ru": "🇷🇺", "rus": "🇷🇺",
    "ro": "🇷🇴", "ron": "🇷🇴",
    "gr": "🇬🇷", "ell": "🇬🇷",
    "hr": "🇭🇷", "hrv": "🇭🇷",
    "rs": "🇷🇸", "srp": "🇷🇸",
    "br": "🇧🇷",
    "ca": "🇨🇦",
    "ar": "🇸🇦", "ara": "🇸🇦",
    "au": "🇦🇺",
}

_FLAG_EMOJI_RE = re.compile(r"[\U0001F1E6-\U0001F1FF]+")
_COUNTRY_PREFIX_RE = re.compile(r"^(?:it|ita|uk|gb|de|fr|es|pt|nl|al|tr|bk)\s*[:|\-]\s*", re.IGNORECASE)
_TRAILING_DOT_TAG_RE = re.compile(r"(?:\s*\.[a-z0-9]{1,3})+$", re.IGNORECASE)
_INLINE_DOT_TAG_RE = re.compile(r"\s*\.[a-z0-9]{1,3}\b", re.IGNORECASE)
_PARENS_TAG_RE = re.compile(r"\([^)]*\)")
_BRACKET_TAG_RE = re.compile(r"\[[^\]]*\]")
_QUALITY_TOKEN_RE = re.compile(r"\b(?:fhd|uhd|sd|hd\+?|hevc|raw|h\.?265|1080p?|720p?)\b", re.IGNORECASE)
_NOISE_WORD_RE = re.compile(r"\b(?:hd|uhd|fhd|4k|sd|tv|channel|canale|live)\b", re.IGNORECASE)
_NON_CHANNEL_CHAR_RE = re.compile(r"[^a-z0-9#+]+")
_TRAILING_TAG_NUM_RE = re.compile(r"\s*\((\d{1,2})\)\s*$")
_ARENA_WORD_RE = re.compile(r"\barena\b", re.IGNORECASE)
_SKY_WORD_RE = re.compile(r"\bsky\b", re.IGNORECASE)
_NUMBER_TOKEN_RE = re.compile(r"\b\d+\b")
_TRAILING_COUNTRY_WORD_RE = re.compile(r"\b([a-z]{2,12})$")

_TAG_RANK_ORDER: dict[str, int] = {"c": 0, "s": 1, "b": 2, "live": 3}
_PAREN_NUM_TO_TAG: dict[str, str] = {"6": "c", "7": "s", "1": "b"}


def _compile_silo_num_index(
    aliases_map: dict[str, str],
) -> dict[frozenset[str], tuple[tuple[re.Pattern[str], str], ...]]:
    buckets: dict[frozenset[str], list[tuple[re.Pattern[str], str]]] = {}
    for alias, canonical in aliases_map.items():
        nums = frozenset(_NUMBER_TOKEN_RE.findall(alias))
        if nums:
            buckets.setdefault(nums, []).append(
                (re.compile(rf"\b{re.escape(alias)}\b"), canonical)
            )
    return {nums: tuple(entries) for nums, entries in buckets.items()}


_SILO_NUM_INDEX: dict[
    int, dict[frozenset[str], tuple[tuple[re.Pattern[str], str], ...]]
] = {
    id(silo_map): _compile_silo_num_index(silo_map)
    for silo_map in (*COUNTRY_ALIASES.values(), GLOBAL_CHANNEL_ALIASES)
}


@lru_cache(maxsize=8192)
def clean_channel_name(name: str) -> str:
    """Clean and normalize a broadcast channel title preserving numbers, `#`, `+`, and `4k`."""
    if not name:
        return ""
    s = _FLAG_EMOJI_RE.sub("", name.strip())
    s = _COUNTRY_PREFIX_RE.sub("", s)
    prev = None
    while prev != s:
        prev = s
        s = _TRAILING_DOT_TAG_RE.sub("", s).strip()
        s = _PARENS_TAG_RE.sub(" ", s).strip()
        s = _BRACKET_TAG_RE.sub(" ", s).strip()
    s = _INLINE_DOT_TAG_RE.sub("", s)
    s = _QUALITY_TOKEN_RE.sub(" ", s.lower())
    s = _NON_CHANNEL_CHAR_RE.sub(" ", s)
    return re.sub(r"\s+", " ", s).strip()


def _match_in_silo(cleaned: str, aliases_map: dict[str, str]) -> Optional[str]:
    if not aliases_map:
        return None
    if cleaned in aliases_map:
        return aliases_map[cleaned]

    no_sym = re.sub(r"\s+", " ", re.sub(r"[#+]", " ", cleaned)).strip()
    if no_sym in aliases_map:
        return aliases_map[no_sym]

    no_noise = re.sub(r"\s+", " ", _NOISE_WORD_RE.sub(" ", cleaned)).strip()
    if no_noise and no_noise in aliases_map:
        return aliases_map[no_noise]

    nums_in_cleaned = frozenset(_NUMBER_TOKEN_RE.findall(cleaned))
    if nums_in_cleaned:
        num_buckets = _SILO_NUM_INDEX.get(id(aliases_map))
        if num_buckets is not None:
            for pattern, canonical in num_buckets.get(nums_in_cleaned, ()):
                if pattern.search(cleaned):
                    return canonical
        else:
            for alias, canonical in aliases_map.items():
                if nums_in_cleaned != frozenset(_NUMBER_TOKEN_RE.findall(alias)):
                    continue
                if re.search(rf"\b{re.escape(alias)}\b", cleaned):
                    return canonical
    return None


@lru_cache(maxsize=8192)
def resolve_canonical_channel(
    channel_name: str,
    country: Optional[str] = None,
) -> Optional[str]:
    """Map a raw channel name to a canonical silo key, returning `None` if unsupported."""
    cleaned = clean_channel_name(channel_name)
    if not cleaned:
        return None

    # Balkan/foreign Arena channels (Arena Sport 1..10, Arena Premium, etc.) must never
    # collide with Italian Sky Sport Arena or German Sport1.
    if _ARENA_WORD_RE.search(cleaned) and not _SKY_WORD_RE.search(cleaned):
        return None

    silo_country = country if country in COUNTRY_ALIASES else None
    trailing = _TRAILING_COUNTRY_WORD_RE.search(cleaned)
    if trailing:
        code = trailing.group(1)
        if code in UNSUPPORTED_COUNTRY_SUFFIXES:
            return None
        if silo_country is None and code in COUNTRY_SUFFIX_MAP:
            detected_country = COUNTRY_SUFFIX_MAP[code]
            silo = COUNTRY_ALIASES.get(detected_country, {})
            base = cleaned[: trailing.start()].strip()
            if base in silo:
                return silo[base]
            if cleaned in silo:
                return silo[cleaned]
            silo_country = detected_country

    candidates: list[dict[str, str]]
    if silo_country:
        candidates = [COUNTRY_ALIASES[silo_country]]
    else:
        candidates = [COUNTRY_ALIASES["Italy"], GLOBAL_CHANNEL_ALIASES]

    for aliases_map in candidates:
        matched = _match_in_silo(cleaned, aliases_map)
        if matched is not None:
            return matched
    return None


@lru_cache(maxsize=8192)
def canonicalize_channel_name(
    channel_name: str,
    country: Optional[str] = None,
) -> str:
    """Resolve a channel name to its canonical key or safe cleaned fallback key."""
    canonical = resolve_canonical_channel(channel_name, country=country)
    if canonical:
        return canonical
    return clean_channel_name(channel_name)


def extract_server_tag(raw_name: str) -> str:
    """Extract mirror priority tag (`c`, `s`, `b`, or `live`) from a raw TvVoo channel name."""
    if not raw_name:
        return "live"
    m_num = _TRAILING_TAG_NUM_RE.search(raw_name)
    if m_num:
        return _PAREN_NUM_TO_TAG.get(m_num.group(1), m_num.group(1))
    lower = raw_name.lower()
    if ".c" in lower:
        return "c"
    if ".s" in lower:
        return "s"
    if ".b" in lower:
        return "b"
    return "live"


def server_tag_rank(raw_name: str) -> int:
    """Return numeric sort rank for a server tag (`.c` < `.s` < `.b` < `live` < other)."""
    return _TAG_RANK_ORDER.get(extract_server_tag(raw_name), 4)


def detect_channel_flag(channel_name: str, language: str = "") -> str:
    """Detect a country flag emoji from channel title keywords or language hint."""
    title = (channel_name or "").strip()
    if title:
        for include_re, exclude_re, flag in CHANNEL_COUNTRY_PATTERNS:
            if include_re.search(title):
                if exclude_re is not None and exclude_re.search(title):
                    continue
                return flag
    lang_key = (language or "").strip().lower()
    return _LANG_FLAG_MAP.get(lang_key, "")


def canonical_from_sky_id(channel_id: Any) -> Optional[str]:
    """Return canonical channel key for a Sky Italia Guida TV API channel ID."""
    return SKY_CHANNEL_IDS.get(str(channel_id).strip())


__all__ = [
    "CANONICAL_DISPLAY_NAMES",
    "CHANNEL_COUNTRY_PATTERNS",
    "COUNTRY_ALIASES",
    "COUNTRY_SUFFIX_MAP",
    "GLOBAL_CHANNEL_ALIASES",
    "SKY_CHANNEL_IDS",
    "SKY_CHID_TO_CANONICAL",
    "TVVOO_COUNTRY_GROUPS",
    "UNSUPPORTED_COUNTRY_SUFFIXES",
    "canonical_from_sky_id",
    "canonicalize_channel_name",
    "clean_channel_name",
    "detect_channel_flag",
    "extract_server_tag",
    "resolve_canonical_channel",
    "server_tag_rank",
]
