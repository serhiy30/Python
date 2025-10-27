# 📊 Daily E-commerce Delta Report

![Python](https://img.shields.io/badge/Python-3.10-blue)
![Databricks](https://img.shields.io/badge/Databricks-SQL-orange)
![Azure](https://img.shields.io/badge/Azure-KeyVault-blueviolet)

## **(ENG)** Englisch Version
---
This repository contains a Databricks notebook for automating daily analysis of price and cost changes in e-commerce products. It compares two versions of a Delta table, calculates percentage changes, aggregates data by brand and vendor, and generates Excel reports and HTML summaries.

# 🚀 Features
---
- Delta Table Version Comparison
Automatically retrieves the latest versions and compares the previous day’s data with today’s data.

- Percentage Change Calculation
Calculates changes in cost and price for each product. Marks significant changes (>5%).

- Top Brands & Vendors Analysis
Aggregates results by top 20 brands and vendors based on sales data.

- HTML & Excel Reporting

- Generates clean, styled HTML tables for quick insights.

- Exports detailed Excel reports with per-MPN level changes.

- Optional Email Automation
Can send automated reports via Microsoft Graph API (OAuth2.0).

# 📦 Tech Stack
---
| Technology | Purpose |
|------------|---------|
| Databricks / Spark SQL | Data aggregation and transformation at scale |
| Python (pandas, openpyxl) | Report generation and formatting |
| Azure Key Vault (dbutils.secrets) | Secure secret management |
| Microsoft Graph API | Automated email and report delivery |
| SQL Views / Temporary Tables | Structured analytics logic |

# 🧠 Example Output
---

| Brand Name | Top 20 Brand | Total MPNs | Cost ↑ >5% | Cost ↑ >5% (%) | Price ↑ >5% | Price ↑ >5% (%) | End Last Promo |
|------------|--------------|------------|------------|----------------|-------------|-----------------|----------------|
| Brand A    | Yes          | 120        | 15         | 12.50%         | 10          | 8.33%           | 2025-10-20     |
| Brand B    | No           | 95         | 5          | 5.26%          | 7           | 7.37%           | -              |
| Brand C    | Yes          | 200        | 25         | 12.50%         | 20          | 10.00%          | 2025-09-15     |
| Brand D    | No           | 80         | 3          | 3.75%          | 6           | 7.50%           | -              |
| Brand E    | Yes          | 150        | 18         | 12.00%         | 12          | 8.00%           | 2025-10-01     |
| Brand F    | Yes          | 110        | 10         | 9.09%          | 8           | 7.27%           | 2025-09-28     |
| Brand G    | No           | 75         | 2          | 2.67%          | 5           | 6.67%           | -              |
| Brand H    | Yes          | 130        | 14         | 10.77%         | 11          | 8.46%           | 2025-10-10     |


Notes:

Red highlights in HTML indicate MPNs with >5% change.

Green indicates Top 20 brands.

End Last Promo shows the last promotion end date, - if none.

# ⚡ Key Highlights
---
Fully automated daily comparison, no manual intervention needed.

Safe for portfolio/demo use — anonymized DB names and domains.

Handles edge cases like empty data, missing prices/costs, zero inventory.

# 📝 Notes
---
Designed for Delta tables in Databricks.

All internal references (prod_db, db.data.gold_tables, etc.) are anonymized for demo purposes.

Email sending is optional; notebook runs safely without it.

# 💡 Potential Improvements
---
Add visualizations of top price/cost changes.

Parameterize the top N brands/vendors dynamically.

Support custom time windows beyond 24-hour delta.

## **(DE)** Deutsche Version

---
Dieses Repository enthält ein Databricks-Notebook zur Automatisierung der täglichen Analyse von Preis- und Kostenänderungen bei E-Commerce-Produkten. Es vergleicht zwei Versionen einer Delta-Tabelle, berechnet prozentuale Änderungen, aggregiert Daten nach Marke und Anbieter und generiert Excel-Berichte und HTML-Zusammenfassungen.

# 🚀 Funktionen
---
- Delta-Tabellen-Versionsvergleich
Ruft automatisch die neuesten Versionen ab und vergleicht die Daten des Vortages mit den Daten des aktuellen Tages.

- Berechnung der prozentualen Veränderung
Berechnet die Kosten- und Preisänderungen für jedes Produkt. Markiert signifikante Änderungen (>5 %).

- Analyse der Top-Marken und -Anbieter
Aggregiert die Ergebnisse nach den 20 führenden Marken und Anbietern auf der Grundlage von Verkaufsdaten.

- HTML- und Excel-Berichte

- Erstellt übersichtliche, formatierte HTML-Tabellen für schnelle Einblicke.

- Exportiert detaillierte Excel-Berichte mit Änderungen auf MPN-Ebene.

- Optionale E-Mail-Automatisierung
Kann automatisierte Berichte über die Microsoft Graph API (OAuth2.0) versenden.

# 📦 Tech Stack
---
| Technologie | Zweck |
|------------|---------|
| Databricks / Spark SQL | Datenaggregation und -transformation in großem Maßstab |
| Python (pandas, openpyxl) | Berichterstellung und Formatierung |
| Azure Key Vault (dbutils.secrets) | Sichere Verwaltung geheimer Daten |
| Microsoft Graph API | Automatisierte E-Mail- und Berichtsübermittlung |
| SQL-Ansichten/temporäre Tabellen | Strukturierte Analyselogik |

# 🧠 Beispielausgabe
---

| Brand Name | Top 20 Brand | Total MPNs | Cost ↑ >5% | Cost ↑ >5% (%) | Price ↑ >5% | Price ↑ >5% (%) | End Last Promo |
|------------|--------------|------------|------------|----------------|-------------|-----------------|----------------|
| Brand A    | Yes          | 120        | 15         | 12.50%         | 10          | 8.33%           | 2025-10-20     |
| Brand B    | No           | 95         | 5          | 5.26%          | 7           | 7.37%           | -              |
| Brand C    | Yes          | 200        | 25         | 12.50%         | 20          | 10.00%          | 2025-09-15     |
| Brand D    | No           | 80         | 3          | 3.75%          | 6           | 7.50%           | -              |
| Brand E    | Yes          | 150        | 18         | 12.00%         | 12          | 8.00%           | 2025-10-01     |
| Brand F    | Yes          | 110        | 10         | 9.09%          | 8           | 7.27%           | 2025-09-28     |
| Brand G    | No           | 75         | 2          | 2.67%          | 5           | 6.67%           | -              |
| Brand H    | Yes          | 130        | 14         | 10.77%         | 11          | 8.46%           | 2025-10-10     |

Hinweise:

Rote Markierungen im HTML-Code kennzeichnen MPNs mit einer Veränderung von >5 %.

Grün kennzeichnet die Top 20 Marken.

„End Last Promo” zeigt das Enddatum der letzten Werbeaktion an, sofern vorhanden.

# ⚡ Wichtigste Highlights
---
Vollautomatischer täglicher Vergleich, kein manueller Eingriff erforderlich.

Sicher für Portfolio-/Demo-Zwecke – anonymisierte DB-Namen und Domains.

Behandelt Sonderfälle wie leere Daten, fehlende Preise/Kosten, Nullbestand.

# 📝 Hinweise
---
Entwickelt für Delta-Tabellen in Databricks.

Alle internen Verweise (prod_db, db.data.gold_tables usw.) sind für Demo-Zwecke anonymisiert.

Der Versand von E-Mails ist optional; das Notebook läuft auch ohne E-Mail-Versand sicher.

# 💡 Mögliche Verbesserungen
---
Visualisierungen der wichtigsten Preis-/Kostenänderungen hinzufügen.

Die wichtigsten N Marken/Anbieter dynamisch parametrisieren.

Benutzerdefinierte Zeitfenster über 24-Stunden-Delta hinaus unterstützen.

