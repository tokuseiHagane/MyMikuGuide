# MyMikuGuide

`MyMikuGuide` - это статический сайт на `Astro`, который публикует локальный snapshot каталога `VocaDB` без выделенного backend-сервера.

## Кратко

- `VocaDB` выступает внешним источником данных.
- `SQLite` в `data/db/vocadb.sqlite` является каноническим локальным snapshot-хранилищем.
- `data/derived/` содержит generated export-артефакты для сайта.
- `GitHub Actions` разносит пайплайн на отдельные стадии `sync` и `publish`.
- `GitHub Pages` публикует уже собранный статический `dist/`.
- Detail-страницы в production ориентированы на browser SQLite snapshot.

## Быстрый старт

```bash
npm install
npm run sync:bootstrap
npm run build
npm run preview
```

Для регулярного локального обновления обычно достаточно:

```bash
npm run sync
npm run build
```

## Основные команды

```bash
npm run dev
npm run preview
npm run check
npm run check:budgets
npm run sync:bootstrap
npm run sync
npm run sync:incremental-hot
npm run sync:reconcile-shard
npm run sync:full
npm run sync:dry-run
npm run build
npm run build:browser-sqlite-snapshot
npm run prepare:local-browser-sqlite
npm run build:local-preview
```

## Data Flow

1. `scripts/sync/index.ts` синхронизирует сущности из `VocaDB`.
2. Канонический локальный snapshot записывается в `data/db/vocadb.sqlite`.
3. Из SQLite материализуются summary/detail/meta JSON-артефакты в `data/derived/`.
4. `Astro` читает `data/derived/` во время `npm run build`.
5. Дополнительно строятся `dist/pagefind` и browser SQLite snapshot.
6. `publish.yml` деплоит итоговый `dist` в `GitHub Pages`.

`sync:full` работает в две фазы: сначала `manifest scan` по каталогам `artists/songs/albums`, затем `hydrate` только для изменившихся или отсутствующих в локальном snapshot сущностей. Состояние обеих фаз сохраняется в `SQLite`, поэтому режим умеет продолжаться после незавершённого прогона.

## Источник истины

- Апстрим-истина по данным остаётся в `VocaDB`.
- Локальная истина внутри проекта - `data/db/vocadb.sqlite`.
- `data/derived/` и `data/raw/vocadb/meta/last-run.json` считаются export/publish-артефактами.
- Generated snapshot-файлы не должны жить в git как постоянное хранилище.

## Структура

- `src/` - Astro-страницы, layout-ы, компоненты и runtime-утилиты.
- `scripts/` - sync, экспорт detail JSON, budgets, поиск и browser SQLite snapshot.
- `data/` - локальная БД, seeds и generated exports.
- `content/` - markdown-архив и content collections.
- `public/` - статические файлы и локальные preview-артефакты.
- `.github/workflows/` - CI/CD pipeline.
- `docs/` - подробная документация по архитектуре, разработке и эксплуатации.

## Документация

- `docs/README.md` - карта документации.
- `docs/architecture.md` - архитектура сайта и структура репозитория.
- `docs/data-pipeline.md` - sync, SQLite, generated artifacts и data flow.
- `docs/development.md` - локальная разработка, команды и переменные окружения.
- `docs/operations.md` - CI/CD, деплой, budgets и runbook.

## Архив

Старый монолитный README сохранён в `content/legacy/readme-archive.md` как исторический архив, но больше не считается рабочим источником документации.
