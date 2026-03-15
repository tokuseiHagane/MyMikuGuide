# Разработка

## Требования

- `Node.js 22` или совместимая версия, совпадающая с CI.
- `npm` для установки зависимостей и запуска скриптов.
- Доступ в сеть к `VocaDB`, если вы запускаете sync.

## Установка

```bash
npm install
```

## Основные команды

### Локальная разработка сайта

```bash
npm run dev
npm run preview
```

### Проверка проекта

```bash
npm run check
npm run check:budgets
```

### Sync

```bash
npm run sync:bootstrap
npm run sync
npm run sync:incremental-hot
npm run sync:reconcile-shard
npm run sync:full
npm run sync:dry-run
```

### Сборка

```bash
npm run build
npm run build:browser-sqlite-snapshot
npm run prepare:local-browser-sqlite
npm run build:local-preview
```

## Практический локальный workflow

Для первого подъёма данных:

1. Установите зависимости.
2. При необходимости скопируйте `.env.example` в `.env`.
3. Выполните `npm run sync:bootstrap`.
4. Затем выполните `npm run build`.
5. Запустите `npm run preview` или `npm run dev`.

Для регулярного локального обновления данных обычно достаточно:

1. `npm run sync`
2. `npm run build`

Если нужен быстрый refresh горячих сущностей, используйте `npm run sync:incremental-hot`.

Если нужно сверить уже известный каталог по шардам, используйте `npm run sync:reconcile-shard`.

Если нужно полностью пересканировать каталог, используйте `npm run sync:full`.

## Что делает `npm run build`

Команда `build` не ограничивается `astro build`.

Последовательно выполняются:

1. `astro build`
2. `scripts/export-client-detail-data.ts`
3. `scripts/build-search.ts`

Из-за этого полноценная сборка ожидает, что `data/derived/` уже подготовлен sync-пайплайном.

## Локальный browser SQLite preview

Если вы хотите проверить production-подобную работу detail-страниц, используйте:

```bash
npm run prepare:local-browser-sqlite
npm run build:local-preview
```

Эти команды создают локальный manifest в `public/meta/db-snapshot.local.json` и snapshot в `public/sqlite/`.

## Переменные окружения

### Часто используемые `SYNC_*`

- `SYNC_CONCURRENCY`
- `SYNC_MAX_NEW_ENTITIES_PER_RUN`
- `SYNC_ROOT_ARTIST_DISCOGRAPHY_LIMIT`
- `SYNC_RELATED_ARTIST_DISCOGRAPHY_LIMIT`
- `SYNC_PROBE_TIMEOUT_MS`
- `SYNC_FETCH_TIMEOUT_MS`
- `SYNC_ID_LOOKUP_TIMEOUT_MS`
- `SYNC_RETRY_FAILED_ON_INCREMENTAL`
- `SYNC_STORE_RAW`
- `SYNC_FORCE_IPV4`
- `SYNC_LOG_PROGRESS`
- `SYNC_LOG_PROGRESS_INTERVAL_MS`

Пример локального fast profile уже дан в `.env.example`.

### Переменные для browser SQLite snapshot

- `PAGES_DB_SOURCE_PATH`
- `PAGES_DB_OUTPUT_ROOT`
- `PAGES_DB_MANIFEST_PATH`
- `PAGES_DB_VERSION`
- `PAGES_DB_UPDATED_AT`
- `PAGES_DB_PAGE_SIZE`
- `PAGES_DB_FILE_NAME`

## Что лежит в git, а что нет

Согласно `.gitignore` в репозиторий не должны попадать:

- локальные `.env`;
- SQLite-файлы в `data/db/`;
- generated detail/summary shards в `data/derived/`;
- локальные browser SQLite preview-артефакты в `public/sqlite/` и `public/meta/db-snapshot.local.json`;
- raw caches в `data/raw/vocadb/*`.

Это означает, что после чистого checkout у вас может не быть данных для локального сайта, пока вы не выполните sync или не подложите snapshot.

## Полезные ориентиры по коду

- `src/lib/site-data.ts` - build-time чтение summary/detail/meta.
- `src/lib/browser-sqlite.ts` - runtime чтение SQLite в браузере.
- `src/lib/client-detail-shell.ts` - отрисовка detail-страниц.
- `scripts/sync/index.ts` - sync orchestration.
- `scripts/build-search.ts` - индексация поиска.
- `scripts/build-browser-sqlite-snapshot.ts` - подготовка browser SQLite.

## Ограничения локальной среды

- Репозиторий привязан к `base: "/MyMikuGuide"` в `astro.config.mjs`, поэтому ассеты и ссылки рассчитаны на Pages-путь этого репозитория.
- Без готового snapshot detail-страницы и каталоги могут показывать пустые fallback-данные, потому что `site-data.ts` безопасно возвращает пустые структуры, если JSON ещё не создан.
