# diafan

Eina de línia de comandes per consultar i descarregar conjunts de dades del [Portal de Transparència de la Generalitat de Catalunya](https://analisi.transparenciacatalunya.cat). Permet consultar metadades, inspeccionar l'estructura, llistar versions històriques i descarregar snapshots en format CSV o JSON.

## Ús ràpid

```
uvx diafan info gn9e-3qhr
uvx diafan schema gn9e-3qhr
uvx diafan versions gn9e-3qhr
uvx diafan download gn9e-3qhr 1352
uvx diafan download-current gn9e-3qhr -f json
```

## Instal·lació

La manera més senzilla d'executar `diafan` és amb `uvx`, que no requereix instal·lació:

```
uvx diafan --help
```

Per instal·lar-lo permanentment:

```
uv tool install diafan
```

O amb `pip`:

```
pip install diafan
```

### Des del codi font

```
git clone https://github.com/davidpoblador/diafan.git
cd diafan
uv sync
```

## Comandes

### `info`

Mostra les metadades d'un conjunt de dades: nom, categoria, atribució, llicència, estadístiques d'ús, dates amb temps relatiu, descripció i etiquetes.

```
uvx diafan info gn9e-3qhr
```

```
╭── Quantitat d'aigua als embassaments de les Conques Internes de Catalunya ───╮
│  Nom                  Quantitat d'aigua als embassaments de les Conques      │
│                       Internes de Catalunya                                  │
│  ID                   gn9e-3qhr                                              │
│  Categoria            Medi Ambient                                           │
│  Atribució            Agència Catalana de l'Aigua (ACA)                      │
│  Enllaç               https://administraciodigital.gencat.cat/ca/dades/dad…  │
│  Publicat per         Dades Obertes Catalunya                                │
│  Procedència          official                                               │
│  Llicència            See Terms of Use                                       │
│  Visualitzacions      372.377                                                │
│  Descàrregues         21.731                                                 │
│  Columnes             5                                                      │
│  Creat                2021-01-22 12:27  (fa 5 anys)                          │
│  Publicat             2023-10-18 11:48  (fa 2 anys)                          │
│  Dades actualitzades  2026-03-11 11:00  (fa 23 hores)                        │
│  Última modificació   2026-03-11 11:00  (fa 23 hores)                        │
│                                                                              │
│  Descripció                                                                  │
│  Estat dels embassaments de les Conques Internes de Catalunya a partir dels  │
│  valors de Nivell de l'aigua a l'embassament, Volum embassat i Percentatge   │
│  del volum embassat respecte la capacitat de l'embassament (dades agregades  │
│  del dia anterior).                                                          │
│                                                                              │
│  Etiquetes                                                                   │
│  #embassaments #nivell #msnm #volum #hm3 #ods6_aigua neta i sanejament       │
│  #ods13_acció pel clima #ods12_producció i consum responsable #aca           │
│  #percentatge                                                                │
╰──────────────────────────────────────────────────────────────────────────────╯
```

### `schema`

Mostra l'estructura (columnes) d'un conjunt de dades, incloent-hi el nom, el nom del camp intern i el tipus de dada.

```
uvx diafan schema gn9e-3qhr
```

```
    Quantitat d'aigua als embassaments de les Conques Internes de Catalunya
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┓
┃ Nom                            ┃ Camp                       ┃ Tipus         ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━┩
│ Dia                            │ dia                        │ calendar_date │
│ Estació                        │ estaci                     │ text          │
│ Nivell absolut (msnm)          │ nivell_absolut             │ number        │
│ Percentatge volum embassat (%) │ percentatge_volum_embassat │ number        │
│ Volum embassat (hm3)           │ volum_embassat             │ number        │
└────────────────────────────────┴────────────────────────────┴───────────────┘
```

### `versions`

Llista les versions arxivades d'un conjunt de dades, de la més recent a la més antiga. Per defecte mostra les 15 més recents.

```
uvx diafan versions gn9e-3qhr
```

```
Quantitat d'aigua als embassaments de les
      Conques Internes de Catalunya
┏━━━━━━━━┳━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━┓
┃ Versió ┃            Creat ┃           ┃
┡━━━━━━━━╇━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━┩
│   1352 │ 2026-03-11 10:00 │ fa 1 dia  │
│   1351 │ 2026-03-10 10:00 │ fa 2 dies │
│   1350 │ 2026-03-09 10:00 │ fa 3 dies │
│   1349 │ 2026-03-08 10:00 │ fa 4 dies │
│   1348 │ 2026-03-07 10:00 │ fa 5 dies │
└────────┴──────────────────┴───────────┘

Mostrant les 5 versions més recents. Feu servir --all per veure-les totes.
```

Per veure totes les versions:

```
uvx diafan versions gn9e-3qhr --all
```

O limitar a un nombre concret:

```
uvx diafan versions gn9e-3qhr --limit 50
```

### `download`

Descarrega una versió específica arxivada d'un conjunt de dades. Activa la materialització de l'arxiu al servidor (si cal) i després descarrega el resultat amb una barra de progrés.

```
uvx diafan download gn9e-3qhr 1352
```

```
Sol·licitant arxiu per gn9e-3qhr v1352...
  Construint arxiu... [00:05] fet
Descarregant: 100%|████████████████| 4.20M/4.20M [00:02<00:00, 1.85MB/s]
Desat a quantitat-d-aigua-als-embassaments-...-gn9e-3qhr-v1352.csv (4.2 MB)
```

Per especificar el fitxer de sortida o el format:

```
uvx diafan download gn9e-3qhr 1352 -o embassaments.csv
uvx diafan download gn9e-3qhr 1352 -f json
```

**Nota:** La materialització d'arxius pot ser lenta o pot fallar per a conjunts de dades molt grans (19M+ files). En aquest cas, feu servir `download-current`.

### `download-current`

Descarrega la versió actual (més recent) d'un conjunt de dades. No requereix materialització d'arxius i funciona sempre, fins i tot amb conjunts de dades molt grans.

```
uvx diafan download-current gn9e-3qhr
```

```
Descarregant snapshot actual de gn9e-3qhr...
Descarregant: 100%|████████████████| 4.20M/4.20M [00:02<00:00, 1.85MB/s]
Desat a quantitat-d-aigua-als-embassaments-...-gn9e-3qhr-actual.csv (4.2 MB)
```

Per descarregar en format JSON:

```
uvx diafan download-current gn9e-3qhr -f json
```

## Format de sortida

Les comandes `download` i `download-current` accepten l'opció `--format` (`-f`) per triar el format:

- `csv` (per defecte)
- `json`

## Llicència

MIT
