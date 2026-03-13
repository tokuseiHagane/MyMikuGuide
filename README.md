# MyMikuGuide

`MyMikuGuide` теперь развивается как статический сайт с автосинхронизацией данных из `VocaDB`.

## Что здесь теперь

- `Astro` как генератор статического сайта.
- `VocaDB` как апстрим фактических данных.
- `GitHub Actions` для автоматического sync/deploy без выделенного сервера.
- `data/raw`, `data/normalized`, `data/derived` как основной конвейер данных.

## Основные команды

```bash
npm install
npm run sync:bootstrap
npm run build
```

## Структура

- `scripts/sync/` — синхронизация и нормализация данных
- `src/pages/` — страницы сайта
- `data/` — локальный снапшот данных
- `content/legacy/` — архив старого монолитного README
- `public/pictures/` — старые локальные изображения, сохранённые для архива

## Архив

Старый монолитный README больше не используется как рабочий источник данных и сохранён в `content/legacy/readme-archive.md`.
