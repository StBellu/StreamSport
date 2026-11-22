# StreamSport: Greenfield Architectural Specification & Master Build Prompt

> Questo documento costituisce la specifica tecnica autonoma e il prompt esecutivo (approccio *Clean-Room*) per guidare un agente AI nella **creazione da zero** di **StreamSport** all'interno di un repository vergine, senza alcun accesso o dipendenza dal vecchio codice sorgente.

---

## 1. Visione, Scopo e Requisiti di Sistema

### 1.1 Obiettivo del Progetto
**StreamSport** è un addon server-side ad alte prestazioni compatibile con il protocollo **Stremio** (e la variante **Nuvio**), dedicato all'aggregazione intelligente di eventi sportivi live, canali TV lineari sportivi ed eventuali sintesi/replay on-demand.
Il sistema agisce come orchestratore centrale:
1. **Ingerisce i calendari ufficiali** da fonti sportive autorevoli garantendo orari esatti, nomi standardizzati e metadati ricchi.
2. **Interroga guide EPG ufficiali** (Sky Italia, Tivùsat) per rilevare trasmissioni televisive lineari e simulcast.
3. **Genera locandine broadcast dinamiche a 16:9** (incluse card specialistiche per il tennis con foto ritagliate dei giocatori, bandiere e sfondi dedicati, e banner di stato Live/Orario).
4. **Scansiona e raccoglie flussi video** da molteplici provider terzi (canali TV 24/7, stream OTT, eventi dedicati, feed P2P AceStream/Torrent), correlandoli all'evento corretto tramite algoritmi di matching euristico bilaterale.
5. **Risolve e serve i flussi a Stremio**, gestendo il bypass dei blocchi DNS censori (AGCOM / Piracy Shield) tramite **DNS-over-HTTPS (DoH)** e delegando il transmuxing DRM ClearKey (DASH $\to$ HLS) e gli header vincolati a **EasyProxy**.

### 1.2 Principi Guida per l'Implementazione
- **Zero Codice Monolitico**: Disaccoppiamento netto tra router HTTP, scheduler di background, servizi di scraping/normalizzazione, rendering grafico e layer di persistenza.
- **Asincronismo Nativo End-to-End**: Architettura asincrona non bloccante (`httpx.AsyncClient`, driver SQLite asincrono `aiosqlite`).
- **Storage Dual-Layer (RAM-First + aiosqlite)**: Lookup istantanei $O(1)$ a latenza zero serviti direttamente dalla cache in memoria RAM per tutte le route critiche di Stremio (`/catalog`, `/meta`, `/stream`), accoppiata a persistenza asincrona SQLite (`aiosqlite`) con `PRAGMA journal_mode = WAL;`, `PRAGMA synchronous = NORMAL;` e `PRAGMA busy_timeout = 30000;` per la tolleranza ai carichi concorrenti.
- **Architettura Top-Down (Schedule First)**: I calendari ufficiali formano l'unica spina dorsale autoritativa del catalogo; i provider di streaming sono "fornitori di sorgenti" che si agganciano agli eventi esistenti, evitando cataloghi caotici generati da scraper eterogenei.
- **Zero-Bandwidth per i Flussi TV Lineari**: Il server addon gestisce il controllo degli accessi e la riscrittura dei manifest HLS, ma non deve fare da proxy per i flussi video pesanti, reindirizzando i chunk video direttamente alle CDN d'origine.

---

## 2. Architettura dei Componenti e Flusso dei Dati

```
 +─────────────────────────────────────────────────────────────────────────────────────────+
 |                                  FONTI DATI ESTERNE                                     |
 |  - Flashscore/Diretta CDN Ninja (Palinsesto Ufficiale IT+EN)                            |
 |  - Sky Italia REST API & Tivùsat Guida TV (EPG Canali Lineari)                          |
 |  - TheSportsDB Web (Locandine ufficiali e calendari)                                    |
 |  - Circuiti Tennis: ESPN API, ATP Tour, WTA CloudFront (Cutout Atleti)                   |
 |  - Provider Flussi: TvVoo/Vavoo, MandraKodi, SPON, DaddyLive, StreamFree, FCTV33, ecc.  |
 +─────────────────────────────────────────────────────────────────────────────────────────+
                                              │
                                              ▼
 +─────────────────────────────────────────────────────────────────────────────────────────+
 |                        MODULO NETWORK & SECURITY (Bypass & DoH)                         |
 |  - DoH Client (Cloudflare/Google DoH con custom socket transport)                       |
 |  - Anti-SSRF Guard (Veto su IP privati RFC 1918, loopback, Docker subnet)               |
 |  - Browser Impersonation (Headers desktop, TLS session via curl_cffi ove necessario)    |
 +─────────────────────────────────────────────────────────────────────────────────────────+
                                              │
                                              ▼
 +─────────────────────────────────────────────────────────────────────────────────────────+
 |                              BACKGROUND SCHEDULER & PIPELINE                            |
 |  1. Ingestion Palinsesto Ufficiale (DirettaService)                                     |
 |  2. Ingestion EPG Broadcast (ItalianEPGService: deduplica simulcast, rimozione repliche)|
 |  3. Harvesting Provider Flussi (Async gather su scraper attivi)                         |
 |  4. Normalizzazione & Matching (TeamMatching bilateral score, alias, demografica)       |
 |  5. Arricchimento Metadati & Locandine (TheSportsDB reconciliation + Tennis Poster Gen) |
 |  6. Classificazione Silo & Cataloghi (GenreClassifier a 5 livelli)                      |
 +─────────────────────────────────────────────────────────────────────────────────────────+
                                              │
                      ┌───────────────────────┴───────────────────────┐
                      ▼                                               ▼
 +─────────────────────────────────────────+     +─────────────────────────────────────────+
 |           IN-MEMORY RAM CACHE           |     |        PERSISTENZA SQLITE (WAL)         |
 |  - Dizionario O(1) Match per ID         |     |  - Tabella matches (dati completi)      |
 |  - Cache Silo TheSportsDB               |     |  - Tabella thesportsdb_cache            |
 |  - Single-Flight Locks per rendering    |     |  - Tabella tennis_athletes_cache        |
 +─────────────────────────────────────────+     +─────────────────────────────────────────+
                      │                                               │
                      └───────────────────────┬───────────────────────┘
                                              │
                                              ▼
 +─────────────────────────────────────────────────────────────────────────────────────────+
 |                             LAYER HTTP & ROUTING STREMIO                                |
 |  - /manifest.json (Stremio) e /nuvio/manifest.json (Nuvio flattened catalogs)           |
 |  - /{config}/catalog/{type}/{id}.json (Filtri genere, ricerca, paginazione)             |
 |  - /{config}/meta/{type}/{id}.json (Dettagli evento, locandina 16:9, descrizione canali)|
 |  - /{config}/stream/{type}/{id}.json (Countdown pre-match, flussi live, replay post)    |
 |  - /live/manifest.m3u8 (Live Manifest Proxy HLS no-freeze con token refresh)           |
 |  - /poster/{match_id}.jpg (Serving dinamico locandine con badge stato/orario)           |
 |  - /configure (Web UI per setup parametri EasyProxy)                                    |
 +─────────────────────────────────────────────────────────────────────────────────────────+
```

---

## 3. Mappa Dettagliata dei Moduli e delle Logiche Funzionali

### 3.1 Modulo Ingestione Palinsesti & Calendario Ufficiale (`DirettaService`)
* **Scopo**: Costituire la base dati primaria autoritativa per orari di gioco, denominazioni corrette delle squadre e competizioni ufficiali.
* **Fonti Utilizzate**:
  * Feed CDN interno di Flashscore / Diretta.it:
    * Feed primario italiano: `https://400.flashscore.ninja/400/x/feed/f_{sport_id}_{day}_3_it_1`
    * Feed secondario inglese: `https://2.flashscore.ninja/2/x/feed/f_{sport_id}_{day}_3_en-uk_1`
  * Parametri finestra temporale: giorni da `-1` a `+2` (copertura da ieri a dopodomani).
* **Protocollo e Formato Dati**:
  * Non è JSON né HTML: è un formato compresso proprietario testuale delimitato da caratteri speciali:
    * `~` separa i singoli blocchi (leghe ed eventi).
    * `¬` separa le coppie chiave/valore all'interno del blocco.
    * `÷` separa la chiave dal valore (`CHIAVE÷VALORE`).
  * Header necessari: User-Agent desktop moderno, `Origin: https://www.diretta.it`, `Referer: https://www.diretta.it/`, e parametro di firma proprietario `x-fsign: SW9D1eZo`.
* **Campi Chiave Estratti**:
  * `ZA`: Nome della lega/competizione.
  * `ZY` / `ZB`: Nazione/Paese organizzatore.
  * `ZAE`: Nome sessione o fase di gara (cruciale per motori e turni di tennis).
  * `AA`: Identificativo univoco del match su Flashscore (es. `O0r8yHkP`).
  * `AD`: Timestamp inizio evento in secondi epoch UTC (da convertire in ms).
  * `AE` / `CX`: Squadra/atleta di casa.
  * `AF`: Squadra/atleta in trasferta.
  * `WU` / `WV`: Slug atleti per il tennis (utilizzati per de-abbreviare i nomi, es. `paolini-jasmine` $\to$ `Jasmine Paolini`).
  * `AL`: Dizionario JSON incorporato dei broadcaster TV ufficiali (chiave `"1"` per canali lineari; ignorare la chiave `"2"` relativa a stream scommesse).
* **Logiche Speciali**:
  * **Deduplicazione Motori**: Flashscore espone una riga `AA` per ogni pilota partecipante a una sessione di F1/MotoGP. È indispensabile aggregarli tramite chiave sessione (`{competizione}_{sessione}_{orario}`) per emettere una sola card evento per Gran Premio.
  * **Dual-Feed Fusion**: Unire il feed IT (prioritario per l'interfaccia utente) con il feed EN per popolare i campi alias (`alt_home`, `alt_away`, `alt_league`), indispensabili per il matching con provider esteri.
  * **Discipline Coperte**: Calcio (1), Tennis (2), Basket (3), Hockey (4), Football Americano (5), Baseball (6), Pallamano (7), Rugby (8), Volley (12), Boxe (16), MMA (28), Formula 1 (32), MotoGP (33).

---

### 3.2 Modulo EPG Canali TV Broadcast (`ItalianEPGService`)
* **Scopo**: Arricchire il catalogo con le trasmissioni sportive in diretta dei broadcaster italiani lineari e integrare eventi speciali orfani (es. sessioni multicampo senza concorrenti fissi, diretta gol, magazine live).
* **Fonti Utilizzate**:
  1. **Sky Italia REST API Ufficiale**:
     * Endpoint: `https://apid.sky.it/gtv/v1/events?from={iso_utc}&to={iso_utc}&pageSize=999&pageNum=0&env=DTH&channels={ch_ids}`
     * *Vincolo Critico*: Il parametro temporale `from` accetta al massimo **3 ore nel passato** rispetto al tempo corrente UTC. Superare tale soglia causa errore bloccante `HTTP 422 Unprocessable Entity`.
     * Canali: 22 canali sportivi (Sky Sport Uno, Calcio, Tennis, Arena, Basket, Max, F1, MotoGP, Golf, 4K, SuperTennis, DAZN 1-4, canali calcio 251-259) + 10 canali generalisti (Rai 1-3, Canale 5, Italia 1, TV8, NOVE, 20 Mediaset, Cielo).
  2. **Tivùsat Guida TV REST API**:
     * Endpoint: `https://services.tivulaguida.it/api/epg/channels/{ch_id}/date/{YYYY-MM-DD}`
     * Monitora: Rai Sport HD, SOLOCALCIO, EQUtv e generalisti in chiaro.
* **Logiche di Filtraggio e Pulizia**:
  * **Filtro Repliche/Studi**: Scarto di notiziari, rassegne stampa, magazine ("Paddock Live", "Calciomercato") tramite regex su titoli e slug.
  * **Filtro Tennis Fuori Calendario**: Interrogare il calendario attivo di Diretta.it per verificare le città dei tornei ATP/WTA attualmente in corso; scartare automaticamente trasmissioni tennistiche relative a tornei non attivi (repliche d'archivio).
  * **Deduplicazione Simulcast**: Se la stessa partita è trasmessa sia in chiaro che su pay-tv (es. TV8 e Sky Sport Uno), fondere i record in una singola scheda evento, estendendo l'orario di validità (`min(start)` $\to$ `max(end)`) e unificando i flussi.
  * **Ghost Event Filter**: Non pubblicare mai eventi derivati da EPG se non esiste almeno un flusso streaming attivo e funzionante associato a quel canale.

---

### 3.3 Modulo Normalizzazione, Tassonomia & Disambiguazione
* **Scopo**: Standardizzare le denominazioni dei tornei, prevenire collisioni tra categorie differenti e classificare correttamente i generi per Stremio.
* **Componenti Logiche**:
  1. **Tassonomia Canonica delle Leghe**:
     * Definire una lista gerarchica di pattern regex ordinati per priorità.
     * **Risoluzione Omonimie**: I campionati sudamericani o minori che contengono la dicitura "Serie A" o "Serie B" (es. Brasileirão Série A, LigaPro Serie A Ecuador) devono essere valutati **prima** dei campionati italiani, con regex a lookaround rigoroso.
     * **Strict Silo Boundary**: Se un evento possiede già uno sport assegnato (es. tennis o basket), non deve mai scattare una regex di calcio o altro sport.
     * **Filtro Tier (Ammissione a 2 Livelli)**: Ammettere solo leghe di primo e secondo livello (Tier 1/2), scartando competizioni minori prive di copertura streaming (es. Serie D, tornei ITF minori).
  2. **Riconoscimento Demografico Rigoroso**:
     * Determinare i flag `is_women` e `is_youth` analizzando **soltanto** il nome della competizione/lega e **mai** i nomi dei giocatori o delle squadre (per evitare che iniziali maschili come "D. Medvedev" o "F. Cobolli" scatenino falsi positivi "Donne/Femminile").
     * Gli eventi femminili o giovanili non devono mai ereditare lo slug della competizione maggiore senior maschile.
  3. **Classificazione Generi a Cascata (5 Livelli)**:
     * *Livello 0*: Early Simulcast (Diretta Gol, Zona DAZN, RedZone) instradati direttamente al catalogo corretto.
     * *Livello 1*: Routing da competizione canonica convalidata.
     * *Livello 2*: Risoluzione dello sport silo (`football`, `basketball`, `tennis`, `motor-sports`, ecc.).
     * *Livello 3*: Sub-classificatori specialistici per sport (separazione campionati nazionali vs internazionali).
     * *Livello 4*: Fallback in `altri_sport`.

---

### 3.4 Modulo Aggregazione & Catalogo Unificato ("Top-Down Direct Linking")
* **Scopo**: Creare la lista unificata di eventi e abbinare in modo deterministico i flussi live recuperati dai vari scraper.
* **Algoritmo di Matching (Team Matching)**:
  1. **Filtro Temporale Rigido**: Discrepanza tra orario ufficiale e orario provider $\le 60\text{ minuti}$.
  2. **Vocabolario Centrale & Alias**: Mappa di sinonimi per club storici privi di corrispondenza testuale diretta (es. `Inter` $\leftrightarrow$ `FC Internazionale Milano`, `Man City` $\leftrightarrow$ `Manchester City`, `Juve` $\leftrightarrow$ `Juventus`).
  3. **Gestione Acronimi e Stopword**: Proteggere acronimi brevi riconosciuti (`bvb`, `psg`, `lafc`, `f1`) e rimuovere parole societarie generiche (`fc`, `ac`, `calcio`, `club`, `united`, `city`, `real`, `atletico`).
  4. **Confronto Bilaterale Invertibile**: Calcolare il punteggio di somiglianza testuale sia per l'ordine diretto ($H_1-H_2$, $A_1-A_2$) sia per l'ordine invertito ($H_1-A_2$, $A_1-H_2$) per gestire formati americani (`Away @ Home`).
  5. **Penalità Demografica**: Applicare un malus severo (es. $-0.30$) se un flusso è etichettato come femminile/giovanile e il match a catalogo è maschile senior (o viceversa).
  6. **Soglie Minime**: Richiedere punteggio medio aggregato $\ge 0.75$ e singolo team $\ge 0.65$ per considerare valido l'abbinamento.
* **Formato ID Univoci Stremio**:
  * Da calendario Diretta: `streamsport:diretta-{flashscore_id}` (es. `streamsport:diretta-O0r8yHkP`).
  * Da EPG: `streamsport:sky_{channel_id}_{slug}_{timestamp}` o `streamsport:tivusat_{...}`.
  * Da Provider orfano convalidato: `streamsport:{provider}:{id}`.

---

### 3.5 Modulo Visual Assets, Locandine & Metadati

#### A. Locandine Generiche & Motori (`TheSportsDBService`)
* **Modalità di Recupero**:
  * **Scraping Autenticato del Calendario (Livello 1)**: Login via sessione HTTP su `thesportsdb.com/user_login.php` (con estrazione CSRF token dalla form) ed estrazione della tabella in `browse_calendar.php?d=YYYY-MM-DD`. Eseguito ogni 12 ore, indicizzato in RAM per silo.
  * **Riconciliazione in-RAM**: 4 gatekeeper (scarto temporale $\le 90$ min, fuzzy match bilaterale $\ge 0.75$, congruenza demografica e compatibilità campionato).
  * **Weekend GP Cascade Fallback (Motori)**: Per le sessioni di F1/MotoGP (prove libere, qualifiche, sprint) che non possiedono una locandina specifica, ereditare automaticamente il poster ufficiale 16:9 della Gara/Gran Premio principale del fine settimana.
  * **Ricerca Semantica Orfani (Livello 2)**: Query web interna su `thesportsdb.com/browse?s={home}+{away}` con validazione icona sport SVG e throttling 1.2s.
  * **Fallback Dark Canvas**: Se non viene reperita alcuna immagine, generare un canvas scuro elegante in tinta unita (RGB 12, 15, 20) con tipografia pulita centrata e nome competizione in grigio subordinato (scartando caratteri a 4 byte per evitare glifi vuoti).

#### B. Pipeline Grafica Locandine Tennis (`TennisPosterService`)
* **Specifiche Canvas**: 1225x700 pixel (formato 16:9), rendering broadcast con Pillow.
* **Fonti Ritagli Atleti (Waterfall)**:
  1. *Cache SQLite con Validazione Attiva*: Controllo URL live in RAM (status 200, aspect ratio verticale per scartare banner).
  2. *ESPN Search API*: `https://site.web.api.espn.com/apis/search/v2?query={nome}&type=player` $\to$ download da CDN ESPN (`a.espncdn.com`).
  3. *Circuiti Ufficiali con Partizione Rigida di Genere*:
     * Maschile (ATP): Chiamata all'API interna `atptour.com/-/www/players/find/byname/{query}/en` con impersonazione Safari 15.5 (`curl_cffi`). Rileva PlayerId e scarica *Gladiator* o *Classic Headshot*. **Filtro Silhouette**: Scartare categoricamente i file di dimensione esatta **4729 bytes** (sagoma grigia predefinita ATP).
     * Femminile (WTA): Chiamata ad `api.wtatennis.com/tennis/players?name={query}`, recupero profilo HTML ed estrazione esclusiva dei cutout CloudFront contenenti il pattern `-Torso_`.
  4. *Sagome Locali Fallback*: Silhouette speculari predefinite per circuito maschile (`Unknow_male.png`) e femminile (`Unknow_female.png`).
* **Tipologie di Composizione**:
  * *Stage Card*: Box in vetro scuro per sessioni generiche o bracket senza atleti definiti.
  * *Country Tie*: Davis/BJK/United Cup con bandiere ufficiali ESPN (500px) ritagliate a 364x236px con angoli arrotondati.
  * *Player Match (Singolare vs Doppio)*: Sfondi ufficiali per torneo (Slam: Wimbledon, Roland Garros, Australian Open, US Open; Coppe; circuiti ATP/WTA/Challenger). Nel singolare, atleti a sinistra e destra (altezza 510px). Nel doppio, composizione a 4 atleti su due livelli prospettici (partner B arretrato a 350px, partner A frontale a 400px). Lower-third banner con effetto vetro ossidiana e cognomi in maiuscolo.

#### C. Banner Service Dinamico (`BannerService`)
* Canvas nativo 889x500 (16:9).
* Sovrimpressione badge di stato:
  * `LIVE`: Pillola rossa con indicatore pulsante.
  * `OGGI / HH:MM`: Pillola dark glass con icona orologio.
  * `REPLAY`: Pillola grigia per eventi archiviati.
  * `TvVoo Ribbon`: Nastro dorato con saetta nera in alto a destra per i match coperti da flussi TvVoo.
* **Single-Flight Lock**: Lock per chiave (`weakref.WeakValueDictionary`) per impedire che richieste concorrenti per la stessa immagine provochino rendering CPU duplicati.

---

### 3.6 Modulo Flussi, Scraper & Strategia di Priorità (`StreamService`)

#### A. Gerarchia e Ordinamento Flussi
I flussi per ciascun evento devono essere ordinati secondo la seguente priorità:
1. **TvVoo / Vavoo (⚡ No-Freeze)**: Canali TV lineari FHD 1080p (priorità a canali italiani Sky, DAZN, Rai, Mediaset).
2. **MandraKodi**: Flussi OTT in chiaro, stream MPEG-DASH ClearKey 1080p 50fps e canali Socceron.
3. **Sportzonline (SPON)**: Flussi HD con priorità assoluta per telecronaca in lingua italiana (`HD7/HD8 ITALIAN`).
4. **DaddyLive (DLHD)**: Eventi e canali 24/7 risolti tramite probe multi-player su EasyProxy (max 30 canali per evento).
5. **StreamFree**: Flussi complementari in lingua inglese (FHD 1080p / SD 540p).
6. **FCTV33**: Flussi multi-sport su rete edge Tencent.
7. **Replay & Sintesi (Post-Partita)**: YouTube (sintesi ufficiali via `yt-dlp`), Dailymotion (API pubblica) e Sport-Video (file torrent completi).
8. **AceStream**: Link P2P esterni in coda come fallback.

#### B. Mappa dei Provider e Meccanismo di Estrazione
- **TvVoo (`tvvoo_service.py`)**:
  - Utilizza le API MediaHubMX v2 (endpoint `/live/resolve` e `mediahubmx-resolve.json` con signature da `vypn.net`).
  - *LiveManifestManager*: Riscrive i manifest HLS convertendo gli URL dei chunk da relativi ad assoluti verso i server CDN Vavoo (consumo di banda video sul server addon = 0%). Gestisce il rinnovo automatico dei token prima della scadenza (evita blocchi a runtime). Fallback HTTP 307 su EasyProxy (`host=Vavoo`).
- **MandraKodi (`mandrakodi_service.py`)**:
  - API con payload cifrato XOR (`MANDRAKODI_API_URL`): estrae canali Sky Sport con DRM ClearKey (`kid:key`), canali DTT in chiaro, eventi last-minute e feed AceStream. Richiede EasyProxy per transmuxing da DASH a HLS.
- **Sportzonline / SPON (`spon_service.py`)**:
  - Scraping plain-text di `sportsonline.st/prog.txt` protetto da DoH. Estrazione sezioni giornaliere, mappatura lingua telecronaca e forwarding su EasyProxy `/proxy/manifest.m3u8`.
- **DaddyLive (`daddylive_api.py`)**:
  - Scraping HTML della schedule giornaliera su mirror attivi con DoH. Risoluzione dei singoli canali delegata a EasyProxy testando a cascata i 7 player disponibili (`dlstreams`).
- **StreamFree (`streamfree_service.py`)**:
  - Discovery mirror da `strmfree.link/api/domains` + estrazione token JavaScript `_0x` dal player. Forwarding manifest su EasyProxy con header `h_referer` e `h_origin`.
- **FCTV33 (`fctv33_service.py`)**:
  - Handshake iniziale per firma `sfver` + query catalogo binario Protobuf su rete TCPWE / Tencent Edge. Inoltro su EasyProxy (`host=fctv33`).
- **YouTube & Dailymotion (`youtube_service.py`, `dailymotion_service.py`)**:
  - Ricerca sintesi e gol post-partita (entro 3-7 giorni). Generazione diretta di master playlist HLS video/audio tramite `yt-dlp` interno, con redirect HTTP 307 verso i domini CDN consentiti (`googlevideo.com`, `dmcdn.net`).
- **Sport-Video (`sportvideo_service.py`)**:
  - Scraping HTML partite integrali da `sport-video.org.ua`, decodifica bencode del file `.torrent`, calcolo `infoHash` SHA-1 e generazione stream BitTorrent per Stremio.
- **Streamed (Rimosso)**:
  - Servizio dismesso e non più attivo upstream: eliminato totalmente da qualsiasi pipeline o file sorgente.

---

### 3.7 Modulo Rete, Bypass e Microservizio Proxy

#### A. DNS-over-HTTPS (`DoHClient`)
* **Problema**: L'autorità delle comunicazioni italiana (AGCOM / Piracy Shield) impone ai provider internet il blocco DNS dei domini di streaming sportivo non autorizzato, reindirizzando le richieste verso IP sinkhole (es. `172.16.0.2`) o restituendo NXDOMAIN.
* **Architettura di Risoluzione**:
  * Il client interroga via HTTPS i resolver fidati di Cloudflare (`https://cloudflare-dns.com/dns-query`) e Google (`https://dns.google/resolve`).
  * **Custom Socket Transport**: Per evitare fallimenti dell'handshake TLS e protezioni Cloudflare/WAF, non bisogna sostituire il dominio nell'URL con l'IP. È necessario implementare un transport personalizzato a livello socket (es. su `httpcore.AnyIOBackend`) che:
    1. Risolve l'hostname in IP pubblico tramite DoH.
    2. Apre la socket TCP direttamente verso l'IP risolto.
    3. Mantiene intatti l'hostname per la negoziazione SNI e l'header HTTP `Host`.
  * **Protezione Anti-SSRF Obbligatoria**: Scartare categoricamente qualsiasi risoluzione che punti a IP privati (RFC 1918: 10.x, 172.16-31.x, 192.168.x), loopback (127.0.0.1), link-local, subnet Docker o metadati cloud (169.254.169.254).

#### B. Integrazione EasyProxy (`realbestia1/EasyProxy`)
* Microservizio esterno fondamentale configurabile tramite `/configure` (`epUrl`, `epPass`):
  * **Transmuxing DASH ClearKey $\to$ HLS**: Converte flussi MPEG-DASH MPD cifrati con chiavi ClearKey (`kid:key`) in flussi HLS standard riproducibili su qualsiasi client Stremio.
  * **Forwarding Header Rigidi**: Inoltra header obbligatori richiesti dai flussi protetti tramite parametri URL:
    `{epUrl}/proxy/manifest.m3u8?url={stream_url}&h_referer={ref}&h_origin={orig}`
  * **Estrattori WAF / JavaScript**: Gestisce l'esecuzione di script ed estrazione iframe per provider protetti da antibot complessi.

---

### 3.8 Architettura di Storage Dual-Layer: RAM Cache e Persistenza Asincrona SQLite

Per garantire contemporaneamente risposte HTTP istantanee verso i player Stremio e persistenza affidabile dei dati, il sistema deve implementare una **struttura a doppio livello (Dual-Layer)**:

#### A. Layer 1: In-Memory RAM Cache (Hot Path - Latenza Zero)
* **Principio Zero-Disk sulle Chiamate Stremio**: Le richieste frequenti inviate dai client (`/catalog/...`, `/meta/...`, `/stream/...`) **non devono mai effettuare query su disco**.
* Il `CatalogService` mantiene costantemente in memoria volatile:
  * Una lista indicizzata di tutti i match attivi per la finestra oraria corrente (-6h a +48h).
  * Un dizionario ad accesso istantaneo `_matches_by_id: dict[str, Match]` per lookup $O(1)$ su ID grezzo e ID con prefisso (`streamsport:...`).
  * Struttura `_calendar_by_silo` per matching immediato con TheSportsDB.
* La RAM viene aggiornata in modo atomico al termine di ogni ciclo di background sync, evitando letture parziali o inconsistenze.

#### B. Layer 2: Persistenza Asincrona con `aiosqlite`
* **Driver Asincrono Nativo**: Impiego di `aiosqlite` integrato nel ciclo di vita `lifespan` di FastAPI (`async with aiosqlite.connect(...)`), eliminando thread-local artigianali e lock globali.
* **Database Unico**: `data/events.db` (o `data/streamsport.db`), rimuovendo tutti i database legacy orfani (`easysports.db`, `matches.db`, ecc.).
* **Configurazione PRAGMA Obbligatoria**:
  * `PRAGMA journal_mode = WAL;`: Consente letture simultanee non bloccanti mentre il background job scrive sul database.
  * `PRAGMA synchronous = NORMAL;`: Ottimizza le scritture su disco preservando la resilienza del file database.
  * `PRAGMA busy_timeout = 30000;`: **Vincolo critico a 30 secondi**. Anche con WAL, SQLite supporta un solo writer per volta. Il timeout di 30000 ms garantisce che se lo scheduler sta effettuando una batch insert di centinaia di eventi, eventuali altre operazioni attendano fino a 30 secondi prima di restituire l'errore `database is locked`, azzerando i crash di concorrenza.
* **Schema Logico**:
  1. `matches`:
     * `id` (PK, stringa), `title`, `category`, `date` (millisecondi UTC), `end_date`, `poster`, `popular`, `competition`, `canonical_competition`, `country`, `sources` (JSON array dei flussi collegati), `teams` (JSON con metadati home/away), `alt_home`, `alt_away`, `alt_league`, `is_women`, `is_youth`, `updated_at`.
     * Indici: `idx_matches_date` su `date`, `idx_matches_cat` su `category`.
  2. `thesportsdb_cache`:
     * `query_key` (PK), `thumb_url`, `status` (`found`/`not_found`), `checked_at`.
     * Indice su `status`.
  3. `tennis_athletes_cache`:
     * `slug` (PK), `provider`, `image_url`, `gender`, `updated_at`.
     * Indice su `slug`.
* **Policy di Retention e Pulizia**:
  * Finestra Live: considerata tra 20 minuti prima dell'inizio e `live_window` minuti dopo (135m calcio, 240m tennis, 180m motori).
  * Epurazione match scaduti: se `ENABLE_REPLAYS=true`, conservare per **72 ore**; se `false`, eliminare dopo **6 ore**.
  * Auto-pulizia cache grafica: eliminare i record `not_found` di TheSportsDB più vecchi di 24 ore per consentire nuovi tentativi.
  * Pulizia filesystem: cancellare le immagini temporanee dei poster e dei ritagli non appena i relativi match vengono rimossi dal database.

---

## 4. Standard Architetturali e Anti-Pattern da Evitare Categoricamente

| Area / Componente | Standard Architetturale Obbligatorio | Anti-Pattern Rigettato |
| :--- | :--- | :--- |
| **Database & Concorrenza** | Adottare un driver SQLite asincrono nativo (**`aiosqlite`**) con connection pool gestito in `lifespan` e `PRAGMA busy_timeout = 30000;`. | Connessioni sincrone `sqlite3` con pool thread-local manuali e lock globali. |
| **Cataloghi Stremio vs Nuvio** | **Mantenere la duplice esposizione funzionale** (cataloghi nested per Stremio, cataloghi flat per Nuvio), centralizzando la logica in un Catalog Engine unico. | Duplicazione dei controller e dei file di routing HTTP per servire client diversi. |
| **Separazione dei Layer** | I router HTTP fungono solo da interfaccia; tutta la logica risiede in servizi applicativi specializzati. | Inserire logica di business pesante (es. elaborazione Pillow o query DB complesse) dentro gli handler di route HTTP. |
| **Igiene del Database** | Un unico file database nominato (`data/streamsport.db`), con pulizia automatica dei match scaduti. | Dispersione di database multipli, orfani o a zero byte nella root di progetto. |
| **Provider Streaming** | Implementare solo provider verificati e attivi. | Mantenere moduli o dipendenze per servizi defunti (es. Streamed). |
| **Resilienza Feed Esterni** | Firma Flashscore (`x-fsign`) e chiavi configurabili da `.env`, con gestione fallback trasparente su EPG. | Parametri crittografici o firme statiche cablate a basso livello nel codice sorgente. |
| **Compatibilità Sky Italia** | Validare la finestra temporale `from` assicurando che non superi mai 170 minuti nel passato. | Inviare query oltre le 3 ore provocando errori HTTP 422 dal server Sky. |
| **Scraping TheSportsDB** | Sessione autenticata gestita con ripristino automatico del login su cookie scaduto e fallback semantico. | Chiamate prive di gestione di sessione o mancata gestione del refresh dei token CSRF. |

---

## 5. Master Prompt Operativo per la Nuova AI

*Copia e incolla il seguente blocco di istruzioni all'agente AI incaricato della riscrittura da zero:*

```markdown
Sei un Senior Software Engineer specializzato in architetture asincrone ad alte prestazioni in Python, protocollo Stremio Addon e sistemi di video streaming broadcast.

Il tuo compito è progettare e sviluppare DA ZERO in questo repository vergine l'addon Stremio/Nuvio ad alte prestazioni "StreamSport", basandoti sulle seguenti specifiche funzionali, contratti di interfaccia e requisiti architetturali di sistema:

1. CORE STACK & ARCHITETTURA:
   - Framework: FastAPI o Litestar con ciclo di vita lifespan asincrono.
   - Storage Dual-Layer (Obbligatorio per prestazioni broadcast):
     * Layer 1 (In-Memory RAM Cache): Dizionario dei match attivi e indicizzazione per ID (_matches_by_id) gestiti in memoria volatile per garantire risposte O(1) a latenza zero sulle route ad altissimo traffico (/catalog, /meta, /stream), senza toccare il disco.
     * Layer 2 (Persistenza Asincrona con aiosqlite): Connessione gestita nativamente in asyncio con PRAGMA journal_mode = WAL, PRAGMA synchronous = NORMAL e PRAGMA busy_timeout = 30000 (30 secondi) per prevenire categoricamente conflitti di concorrenza (database is locked) tra le scritture massive dello scheduler e le letture di fallback.
   - Client HTTP: httpx asincrono con custom transport per DNS-over-HTTPS (DoH Cloudflare/Google) e protezione rigorosa anti-SSRF.
   - Rendering Grafico: Pillow (PIL) asincronizzato su threadpool per locandine 16:9 con single-flight locks per match.
   - Containerizzazione: Dockerfile minimale basato su python:3.12-slim con tzdata, curl e font DejaVu.

2. LOGICHE CHIAVE DA IMPLEMENTARE:
   - Schedule Ingestion (Top-Down): Ingerire il palinsesto ufficiale da Flashscore/Diretta.it interrogando i feed proprietari delimitati (~, ¬, ÷) su CDN ninja (feed 400 IT + feed 2 EN). Deduplicare le sessioni di F1/MotoGP in un'unica card per evento.
   - EPG Broadcast Italiano: Interrogare le API REST ufficiali di Sky Italia (rispettando il limite rigido di max 3 ore nel passato per evitare HTTP 422) e Tivùsat Guida TV. Deduplicare i simulcast e filtrare repliche/magazine.
   - Disambiguazione & Tassonomia: Implementare la tassonomia canonica delle leghe con confini rigidi di silo sportivo (strict silo boundary). La rilevazione demografica (donne/giovanili) deve analizzare SOLO il nome del torneo, mai i nomi degli atleti.
   - Visual Assets & Poster:
     * Sport generici: Scraping Web autenticato TheSportsDB (login con sessione e CSRF su browse_calendar.php + fallback ricerca semantica web) e riconciliazione in-RAM (4 gatekeeper: tempo, fuzzy match bilaterale, demografia, competizione). Per i motori applicare il Weekend GP Cascade Fallback. Fallback su dark canvas pulito.
     * Tennis Poster Service: Canvas 1225x700 16:9. Recupero ritagli atleti con waterfall: Cache DB validata live -> ESPN Search API -> API ATP Tour (curl_cffi Safari 15.5, scartando file di 4729 bytes) -> WTA CloudFront (-Torso_). Layout differenziati per singolare e doppio con profondità di campo e bandiere nazioni ESPN 500px.
     * Banner Service: Badge dinamici LIVE / ORARIO / REPLAY e ribbon TvVoo.
   - Stream Harvesting & Priorità:
     * TvVoo/Vavoo: LiveManifestManager per riscrittura HLS e auto-rinnovo token (0% banda video).
     * MandraKodi: Parsing stream MPEG-DASH ClearKey e canali DTT.
     * SPON: Scraping prog.txt con estrazione telecronaca italiana (HD7/HD8).
     * DaddyLive & StreamFree: Scraping con DoH e delega risoluzione/proxy a EasyProxy.
     * Rimozione Streamed: Eliminazione totale e definitiva (servizio offline, nessun codice residuo).
     * Integrazione EasyProxy: Transmuxing DASH ClearKey -> HLS e forwarding header (h_referer, h_origin).
   - Protocollo Stremio & Nuvio (Doppia Esposizione Cataloghi):
     * Mantenere la distinzione richiesta dai client: cataloghi con filtri genere annidati (extra: [{"name": "genre"}]) per Stremio, e cataloghi appiattiti indipendenti (flat catalog definitions) per Nuvio per prevenire crash e problemi di navigazione nei dropdown.
     * Centralizzare la logica di business nel Catalog Engine per evitare duplicazione di codice nelle route.
     * Finestra Live: countdown informativo per eventi >20 min, link live attivi durante il match, replay opzionali (YouTube/Dailymotion/Torrent) a fine partita.

3. REQUISITI DI QUALITÀ DEL CODICE:
   - Modularità assoluta: ogni provider di flussi e ogni scraper di calendari deve risiedere in un file autonomo con interfacce chiare.
   - Tipizzazione statica completa con Pydantic v2 e Type Hints Python.
   - Zero codice morto o database duplicati: un solo file database (data/streamsport.db).
   - Logging strutturato e gestione resiliente dei fallimenti esterni con fallback a cascata trasparenti.
```
