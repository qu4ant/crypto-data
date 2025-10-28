# 📊 Crypto Data

**Infrastructure de données pour la cryptomonnaie** - Téléchargement automatique des données OHLCV multi-exchanges et classements de marché.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Version](https://img.shields.io/badge/version-4.0.0-green.svg)](https://github.com/qu4ant/crypto-data)

---

## 🎯 Vue d'ensemble

**Crypto Data** est un pipeline d'ingestion qui télécharge automatiquement les données de marché crypto et les stocke dans une base de données DuckDB locale.

✨ **Philosophie** : Ce package fait **UNE chose** - peupler une base de données. Vous interrogez ensuite directement la base avec SQL.

### Fonctionnalités principales

- 📈 **Données OHLCV** : Téléchargement depuis Binance Data Vision (spot + futures)
- 🌍 **Classements univers** : Top N cryptos par capitalisation via CoinMarketCap
- 🚀 **Téléchargements asynchrones** : 20 téléchargements parallèles pour vitesse maximale
- 🔄 **Gestion automatique** : Détection de format, retry intelligent, gestion des tokens 1000-prefix
- 💾 **DuckDB** : Base de données embarquée, requêtes SQL rapides
- 🏗️ **Multi-exchange ready** : Schéma v4.0.0 préparé pour Bybit, Kraken, etc.

---

## 🔄 Schéma Input/Output

```
┌─────────────────────────────────────────────────────────────┐
│                         INPUTS                               │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  📊 CoinMarketCap API                                       │
│  └─> Classements top N par capitalisation                  │
│      (résout le biais du survivant)                        │
│                                                              │
│  📈 Binance Data Vision                                     │
│  └─> Données OHLCV historiques                             │
│      (spot + futures, intervalles 5m/1h/4h/1d)             │
│                                                              │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                   CRYPTO-DATA PIPELINE                       │
│                                                              │
│  ⚙️  Téléchargement asynchrone (20 threads)                 │
│  ⚙️  Auto-détection format timestamps                       │
│  ⚙️  Gestion 1000-prefix (PEPE, SHIB, BONK)                │
│  ⚙️  Transaction atomique par symbole                       │
│                                                              │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                         OUTPUT                               │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  💾 crypto_data.db (DuckDB)                                 │
│                                                              │
│  Tables:                                                     │
│  • crypto_universe  → Classements historiques               │
│  • spot             → Prix OHLCV spot                       │
│  • futures          → Prix OHLCV futures                    │
│                                                              │
│  📝 Vous interrogez avec SQL:                               │
│                                                              │
│  SELECT symbol, close, volume                               │
│  FROM spot                                                   │
│  WHERE exchange = 'binance'                                 │
│    AND symbol = 'BTCUSDT'                                   │
│    AND interval = '1h'                                      │
│    AND timestamp >= '2024-01-01'                            │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

---

## ⚠️ Pourquoi ce projet? Le biais du survivant

### Le problème

Imaginez que vous analysez les cryptos en **ne prenant que les top 100 d'aujourd'hui**. Votre analyse ignore complètement les cryptos qui étaient dans le top 100 avant mais ont disparu :

- **FTX Token (FTT)** : #25 en 2022, effondrement en novembre 2022
- **Terra LUNA** : #10 en 2022, crash catastrophique en mai 2022
- **Bitconnect (BCC)** : Top crypto, scam révélé en 2018

Si vous n'incluez pas ces cryptos dans votre backtest, vos résultats seront **artificiellement optimistes** - c'est le **biais du survivant**.

### La solution : CoinMarketCap + Stratégie UNION

✅ **crypto-data** résout ce problème en :

1. **Téléchargeant les classements historiques** via CoinMarketCap chaque mois
2. **Utilisant une stratégie UNION** : récupère TOUS les symboles qui ont été dans le top N **à n'importe quel moment** de la période

**Exemple concret** :
```python
# Top 100 sur 12 mois
get_symbols_from_universe(
    db_path='crypto_data.db',
    start_date='2024-01-01',
    end_date='2024-12-31',
    top_n=100
)
# Résultat : ~120-150 symboles
# (100 actuels + entrées/sorties du top 100)
```

Vous capturez ainsi **toute la dynamique du marché** : entrées, sorties, échecs, delistings.

---

## 🚀 Installation

```bash
pip install crypto-data
```

### Installation pour développeurs

```bash
git clone https://github.com/qu4ant/crypto-data.git
cd crypto-data
pip install -e ".[dev]"
```

**Dépendances** : Python 3.8+, duckdb, aiohttp, pandas, requests

---

## 💻 Démarrage rapide

### Option 1 : Workflow complet avec `sync()`

La fonction `sync()` fait tout en un appel : télécharge l'univers + données OHLCV.

```python
from crypto_data import sync, setup_colored_logging

# Logs colorés (optionnel mais recommandé)
setup_colored_logging()

# Téléchargement complet
sync(
    db_path='crypto_data.db',
    start_date='2024-01-01',
    end_date='2024-12-31',
    top_n=100,                    # Top 100 par capitalisation
    interval='1h',                # 5m, 1h, 4h, 1d
    data_types=['spot', 'futures'],
    exclude_tags=['stablecoin', 'wrapped-tokens'],  # Filtres optionnels
    exclude_symbols=['LUNA', 'FTT', 'UST']
)
```

### Option 2 : Étape par étape

```python
import asyncio
from crypto_data import (
    ingest_universe,
    get_symbols_from_universe,
    ingest_binance_async,
    setup_colored_logging
)

setup_colored_logging()

# 1. Télécharger classements CoinMarketCap (async, téléchargements parallèles)
asyncio.run(ingest_universe(
    db_path='crypto_data.db',
    months=['2024-01', '2024-02', '2024-03'],  # Liste de mois à télécharger
    top_n=100,
    exclude_tags=['stablecoin'],
    exclude_symbols=[]
))

# 2. Extraire symboles avec stratégie UNION
symbols = get_symbols_from_universe(
    db_path='crypto_data.db',
    start_date='2024-01-01',
    end_date='2024-12-31',
    top_n=100
)

# 3. Télécharger données Binance (asynchrone)
ingest_binance_async(
    db_path='crypto_data.db',
    symbols=symbols,
    start_date='2024-01-01',
    end_date='2024-12-31',
    data_types=['spot', 'futures'],
    interval='1h'
)
```

---

## 📊 Exemples de requêtes SQL

Une fois les données téléchargées, interrogez directement avec SQL (DuckDB, pandas, Jupyter...).

### 1. Liste des symboles disponibles

```sql
SELECT DISTINCT symbol, COUNT(*) as nb_rows
FROM spot
WHERE exchange = 'binance'
  AND interval = '1h'
GROUP BY symbol
ORDER BY nb_rows DESC;
```

### 2. Historique de prix Bitcoin

```sql
SELECT
    timestamp,
    open,
    high,
    low,
    close,
    volume
FROM spot
WHERE exchange = 'binance'
  AND symbol = 'BTCUSDT'
  AND interval = '1h'
  AND timestamp >= '2024-01-01'
ORDER BY timestamp;
```

### 3. Joindre univers + prix (capitalisation)

```sql
SELECT
    u.date,
    u.symbol,
    u.rank,
    u.market_cap,
    s.close,
    s.volume
FROM crypto_universe u
JOIN spot s
  ON u.symbol || 'USDT' = s.symbol
  AND u.date = DATE_TRUNC('day', s.timestamp)
WHERE s.exchange = 'binance'
  AND s.interval = '1h'
  AND u.date >= '2024-01-01'
ORDER BY u.date, u.rank;
```

### 4. Analyse de volume par exchange (futur)

```sql
-- Aujourd'hui : seulement Binance
-- Futur : comparer Binance vs Bybit vs Kraken
SELECT
    exchange,
    symbol,
    interval,
    SUM(volume) as total_volume
FROM spot
WHERE timestamp >= '2024-01-01'
GROUP BY exchange, symbol, interval
ORDER BY total_volume DESC;
```

### 5. Top 10 cryptos par volume (24h)

```sql
SELECT
    symbol,
    SUM(volume) as volume_24h,
    AVG(close) as avg_price
FROM spot
WHERE exchange = 'binance'
  AND interval = '1h'
  AND timestamp >= NOW() - INTERVAL '24 hours'
GROUP BY symbol
ORDER BY volume_24h DESC
LIMIT 10;
```

---

## 🗄️ Schéma de base de données (v4.0.0)

Un seul fichier : `crypto_data.db`

### Table `crypto_universe` - Classements historiques

Stocke les classements CoinMarketCap (top N par capitalisation).

| Colonne      | Type      | Description                                      |
|--------------|-----------|--------------------------------------------------|
| `date`       | DATE      | Date du classement (mensuel)                     |
| `symbol`     | VARCHAR   | Symbole base (BTC, ETH, pas BTCUSDT)             |
| `rank`       | INTEGER   | Classement par capitalisation                    |
| `market_cap` | DOUBLE    | Capitalisation en USD                            |
| `categories` | VARCHAR   | Tags CoinMarketCap (stablecoin, DeFi, etc.)      |

**Clé primaire** : `(date, symbol)`
**Index** : `(date, rank)`

### Tables `spot` et `futures` - Données OHLCV

Données de prix historiques multi-exchanges (actuellement Binance uniquement).

| Colonne           | Type      | Description                                   |
|-------------------|-----------|-----------------------------------------------|
| `exchange`        | VARCHAR   | Exchange ('binance', futur: 'bybit', etc.)    |
| `symbol`          | VARCHAR   | Paire de trading (BTCUSDT, ETHUSDT, etc.)     |
| `interval`        | VARCHAR   | Intervalle (5m, 1h, 4h, 1d)                   |
| `timestamp`       | TIMESTAMP | Timestamp de la bougie                        |
| `open`            | DOUBLE    | Prix d'ouverture                              |
| `high`            | DOUBLE    | Prix maximum                                  |
| `low`             | DOUBLE    | Prix minimum                                  |
| `close`           | DOUBLE    | Prix de clôture                               |
| `volume`          | DOUBLE    | Volume en base asset                          |
| `quote_volume`    | DOUBLE    | Volume en quote asset (USDT)                  |
| `trades_count`    | INTEGER   | Nombre de trades                              |
| `taker_buy_*`     | DOUBLE    | Volumes d'achat taker                         |

**Clé primaire** : `(exchange, symbol, interval, timestamp)`
**Index** : `(exchange, symbol, interval, timestamp)`

---

## 🔧 Fonctionnalités avancées

### 🤖 Gestion automatique intelligente

Le pipeline gère automatiquement plusieurs problèmes de données :

#### 1. **Formats de timestamps variables**
- 2024 : millisecondes (13 chiffres)
- 2025 : microsecondes (16 chiffres)
- ✅ Détection automatique et conversion

#### 2. **En-têtes CSV inconsistants**
- Certains fichiers ont des en-têtes, d'autres non
- ✅ Détection automatique par analyse de la première ligne

#### 3. **Tokens 1000-prefix (PEPE, SHIB, BONK)**
- Futures : `1000PEPEUSDT`, Spot : `PEPEUSDT`
- ✅ Retry automatique avec prefix, normalisation dans la base

#### 4. **Détection de delisting**
- FTT delisté en novembre 2022
- ✅ Arrêt après 3 échecs consécutifs (seuil configurable)

### 🎯 Décisions de design

**Multi-exchange v4.0.0** : Schéma prêt pour Bybit, Kraken, Coinbase
- Colonne `exchange` dans la clé primaire
- Analyses cross-exchange, détection d'arbitrage, redondance

**Rebrands = symboles séparés** : MATIC→POL, RNDR→RENDER traités différemment
- Raison : interruptions de trading, fichiers séparés, liquidité différente
- Solution : requêtes UNION pour combiner les périodes

**Stratégie UNION** : capture TOUS les symboles du top N sur la période
- ~120-150 symboles pour top 100 sur 12 mois
- Évite le biais du survivant

**Paramètres explicites** : Pas de fichiers config cachés
- `exclude_tags` et `exclude_symbols` explicites dans chaque appel
- Meilleure testabilité, zéro dépendance cachée

---

## 📚 Documentation supplémentaire

- 📖 [CLAUDE.md](CLAUDE.md) - Documentation technique complète pour développeurs
- 📓 [Jupyter Notebook](exemples/explore_crypto_data.ipynb) - Exemples de requêtes et visualisations
- 🐛 [GitHub Issues](https://github.com/qu4ant/crypto-data/issues) - Rapporter un bug

---

## 🧪 Tests

```bash
# Tous les tests
pytest tests/ -v

# Tests basiques uniquement
pytest tests/database/test_database_basic.py -v

# Avec couverture
pytest tests/ --cov=crypto_data --cov-report=html
```

---

## 📝 Licence

**MIT License** - Copyright (c) 2025 Crypto Data Contributors

Voir [LICENSE](LICENSE) pour les détails.

---

## 🤝 Contribution

Les contributions sont les bienvenues! Pour contribuer :

1. Fork le projet
2. Créez une branche feature (`git checkout -b feature/AmazingFeature`)
3. Commit vos changements (`git commit -m 'Add AmazingFeature'`)
4. Push vers la branche (`git push origin feature/AmazingFeature`)
5. Ouvrez une Pull Request

**Philosophie** : Simplicité > Fonctionnalités. Ce package fait **une chose** : ingestion de données. Pas de loaders/readers/query helpers.

---

## ⚡ Pourquoi crypto-data?

✅ **Simple** : Une fonction pour tout télécharger
✅ **Rapide** : 20 téléchargements parallèles
✅ **Fiable** : Retry automatique, gestion d'erreurs intelligente
✅ **Sans biais** : Stratégie UNION capture tous les symboles historiques
✅ **SQL-first** : Requêtes directes, pas d'abstraction inutile
✅ **Multi-exchange ready** : Schéma v4.0.0 préparé pour l'avenir

---

**Développé avec ❤️ pour la communauté quant/crypto**
