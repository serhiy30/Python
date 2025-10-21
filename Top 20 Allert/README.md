**ENG**

**Project Title**

Automated Inventory & Pricing Change Reporting System

Description:
This project is an automated analytics and reporting system designed to track and report changes in product inventory, costs, and prices using Databricks (Spark SQL).
It consolidates multiple data sources, calculates percentage changes in key metrics, identifies significant deviations, and generates detailed Excel reports for analytical and management review.

The system also integrates with Microsoft Graph API to automatically send email notifications with attached reports. All credentials and tokens are securely stored in Azure Key Vault via dbutils.secrets.

**Key Features**
- Comparison of inventory and pricing data between current and previous periods.

- Calculation of percentage changes and detection of significant deviations (>5%).

- Aggregated reporting by brand, category, and manufacturer.

- Automated Excel report generation (including “Salary Report” and “Detailed Salary Report”) with formatting.

- Email delivery automation through Microsoft Graph API (Outlook / Microsoft 365).

- Secure credential management using Databricks Secrets and Azure Key Vault.

**Technologies**

- Databricks / Spark SQL — data aggregation and transformation at scale.

- Python (pandas, openpyxl) — report generation and formatting.

- Azure Key Vault (dbutils.secrets) — secure secret management.

- Microsoft Graph API — automated email and report delivery.

- SQL Views / Temporary Tables — structured analytics logic.

**Outcome**

The system fully automated the process of monitoring inventory and pricing changes, significantly reducing manual analyst workload and ensuring timely delivery of actionable insights to stakeholders.

**DE**

**Projekttitel**

Automatisiertes Berichtswesen für Bestands- und Preisänderungen

Beschreibung:
Dieses Projekt ist ein automatisiertes Analyse- und Berichtswesen, das dazu dient, Änderungen im Produktbestand, bei den Kosten und Preisen mithilfe von Databricks (Spark SQL) zu verfolgen und zu melden.
Es konsolidiert mehrere Datenquellen, berechnet prozentuale Änderungen bei wichtigen Kennzahlen, identifiziert signifikante Abweichungen und erstellt detaillierte Excel-Berichte für die Analyse und Überprüfung durch das Management.

Das System ist außerdem mit der Microsoft Graph API integriert, um automatisch E-Mail-Benachrichtigungen mit angehängten Berichten zu versenden. Alle Anmeldedaten und Tokens werden über dbutils.secrets sicher in Azure Key Vault gespeichert.

**Wichtigste Funktionen**
- Vergleich der Bestands- und Preisdaten zwischen aktuellen und früheren Zeiträumen.

- Berechnung prozentualer Veränderungen und Erkennung signifikanter Abweichungen (>5 %).

- Aggregierte Berichterstellung nach Marke, Kategorie und Hersteller.

- Automatisierte Erstellung von Excel-Berichten (einschließlich „Gehaltsbericht” und „Detaillierter Gehaltsbericht”) mit Formatierung.

- Automatisierung der E-Mail-Zustellung über die Microsoft Graph API (Outlook / Microsoft 365).

- Sichere Verwaltung von Anmeldedaten mit Databricks Secrets und Azure Key Vault.

**Technologien**

- Databricks / Spark SQL – Datenaggregation und -transformation in großem Maßstab.

- Python (pandas, openpyxl) – Berichterstellung und Formatierung.

- Azure Key Vault (dbutils.secrets) – sichere Verwaltung geheimer Daten.

- Microsoft Graph API – automatisierte E-Mail- und Berichtsübermittlung.

- SQL-Ansichten/temporäre Tabellen – strukturierte Analyselogik.

**Ergebnis**

Das System hat den Prozess der Überwachung von Bestands- und Preisänderungen vollständig automatisiert, wodurch sich der manuelle Arbeitsaufwand für Analysten erheblich reduziert hat und sichergestellt ist, dass den Stakeholdern zeitnah umsetzbare Erkenntnisse zur Verfügung stehen.
