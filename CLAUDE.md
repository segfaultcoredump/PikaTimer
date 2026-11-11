# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

PikaTimer is a JavaFX race timing desktop application (GPLv3). Java 25, Maven, main class `com.pikatimer.Pikatimer`. An event is stored in a single embedded H2 file database. There are no unit tests (`src/test/java` is empty).

## Commands

- Run the app: `mvn clean javafx:run` (debug with JDWP on port 8000: `mvn clean javafx:run@debug`)
- Build: `mvn -B package` (the shade plugin produces the runnable fat jar; CI runs exactly this on JDK 25 with JavaFX, e.g. Zulu `jdk+fx`)
- Releases: `.github/workflows` builds native installers with jDeploy on a published GitHub release. jDeploy config (JVM args, Java version, version string) lives in `package.json`.
- The JavaFX module needs `--add-opens javafx.graphics/javafx.scene=ALL-UNNAMED` (ControlsFX). It's already set in the pom and `package.json`; keep it if you launch another way.

## Architecture

Package root `src/main/java/com/pikatimer`, split by domain: `event`, `participant`, `race`, `timing` (with `reader`), `results` (with `reports`), `util` (with `fileTransports`). FXML views and their `FXML*Controller` classes sit beside the domain code, and the layouts are under `src/main/resources/com/pikatimer`.

- **Persistence**: Hibernate 6.6 (`jakarta.persistence`) with annotated entities, backed by H2 and pooled by c3p0. Bulk loads of entities that are used detached go through `HibernateUtil.loadReadOnly`. `util/HibernateUtil.getConfiguration()` **explicitly registers every entity with `addAnnotatedClass`**, so a new entity must be added there. The schema is managed by **Flyway** migrations in `src/main/resources/db/migration` (`B2__baseline.sql` plus `V2_0_x` scripts), which run when an event file is opened (`FXMLopenEventController`). Schema changes need a new migration, because Hibernate does not generate the schema.
- **DAOs**: `*DAO` classes (`RaceDAO`, `TimingDAO`, `ResultsDAO`, `EventDAO`, ...) are lazy-holder singletons obtained with `getInstance()`. They wrap the Hibernate sessions, and the UI and processing code go through them.
- **Domain models use JavaFX properties/observable collections** so the UI binds to them directly. Be careful about which thread you touch them from (JavaFX application thread vs. background processing).
- **Timing pipeline**: `timing/reader` implements `TimingReader` for each hardware or file input type (RFID Ultra, Joey, etc.; see `TimingInputTypes`). Readers produce `RawTimeData`, which is mapped through `Bib2ChipMap`, skew and backup flags into `CookedTimeData`. `TimingListener` notifies downstream consumers. `TimeOverride` handles per-split overrides.
- **Results**: `results/ResultsDAO` and `Result`/`ProcessedResult` turn the cooked times plus the race definition (`Race`, `Wave`, `Split`, `Segment`, the age/sex groups and the awards definitions) into results. `results/reports` holds the report generators (text, HTML/DataTables, awards). `RaceOutputTarget`, `RaceReport` and `ReportDestination` define where reports go. `util/fileTransports` uploads them over FTP/FTPS/SFTP (commons-net, JSch). `util/HTTPServices` serves data via Javalin and WebSocket, e.g. the Marmot announcer feed.
- **Preferences/startup**: `Pikatimer.java` (app entry, JDBC URL, event file handling) and `PikaPreferences`.

`src/main/java/com/pikatimer/TODO.txt` tracks the 2.0 release plan. The git branch `v2.0` is the current development line, and `master` is the main branch.
